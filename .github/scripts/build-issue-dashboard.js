// Living Triage Board — a single GitHub Issue that behaves like an app.
//
// Every ~5 minutes an Action runs this. It:
//   1. Reads all open PRs of the target repo (GraphQL) and classifies each
//      reviewer as auto-assigned (from the auto-assign bot comment) or manual,
//      first-pass or maintainer, with their latest review status.
//   2. Reads the dashboard issue's *comments*, which are one-line commands that
//      anyone (no repo write access needed) can post: /done, /undone, /note,
//      /priority, /claim, /unclaim. It applies them to the board state, then
//      DELETES each processed command comment so the issue never accrues a pile
//      of comments (that was the "page gets slow" risk).
//   3. Persists canonical human state in a hidden JSON block inside the issue
//      body, so it survives every regeneration. Humans express *intent* via
//      commands; the bot owns the *record*.
//   4. Rewrites the issue body: live shields.io badges, a Mermaid chart,
//      priority-grouped tables, and a collapsible per-reviewer "your queue".
//
// Invoked from a github-script step:
//   const build = require('./.github/scripts/build-issue-dashboard.js');
//   await build({ github, context, core });
//
// Env:
//   DASHBOARD_ISSUE  required — issue number to own (in the repo the Action runs in).
//   TARGET_REPO      optional "owner/name" — read PRs from here instead of this repo.
//   POOLS_PATH       optional — path to reviewer-pools.json (default .github/reviewer-pools.json).

const fs = require('fs');

// One-time priority seed, transcribed from Allen's triage doc. Only used for a
// PR the board has never seen before; once a PR has state its stored value wins
// (including a human clearing it), so the doc is never re-read.
const SEED_PRIORITY = {
  984: 1, 985: 1, 997: 1, 1001: 1, 1083: 1, 1084: 1, 1090: 1, 1217: 1, 1263: 1,
  1287: 1, 1321: 1, 1292: 1, 1301: 1, 1302: 1, 1279: 1, 924: 1, 1068: 1, 1075: 1,
  1196: 1, 1204: 1, 1271: 1, 1273: 1, 1277: 1, 1295: 1, 1304: 1,
  1184: 2, 567: 2, 759: 2, 800: 2, 923: 2, 1023: 2, 1193: 2, 1218: 2, 1239: 2,
  1296: 2, 1103: 2, 1297: 2,
  1278: 3, 762: 3, 864: 3, 908: 3, 922: 3, 936: 3, 946: 3, 947: 3, 948: 3, 949: 3,
  967: 3, 983: 3, 1000: 3, 1021: 3, 1116: 3, 1129: 3, 1139: 3, 1225: 3, 1234: 3,
  1236: 3, 1268: 3, 1294: 3, 1035: 3, 1088: 3,
  795: 4, 859: 4, 988: 4, 1005: 4, 1113: 4, 1016: 4, 1049: 4, 1256: 4, 1300: 4,
  1095: 4, 1124: 4, 1298: 4, 894: 4,
};

const STALE_DAYS = 7;

// ── GitHub data gathering ────────────────────────────────────────────────────
async function gatherPRs(github, owner, repo) {
  const query = `
    query($owner:String!, $repo:String!, $cursor:String) {
      repository(owner:$owner, name:$repo) {
        pullRequests(states:OPEN, first:25, after:$cursor, orderBy:{field:CREATED_AT, direction:ASC}) {
          pageInfo { hasNextPage endCursor }
          nodes {
            number title url isDraft createdAt updatedAt
            author { login }
            labels(first:30) { nodes { name } }
            reviewRequests(first:30) { nodes { requestedReviewer { __typename ... on User { login } } } }
            reviews(first:100) { nodes { author { login } state submittedAt } }
            comments(first:100) { nodes { author { login } body } }
          }
        }
      }
    }`;
  const nodes = [];
  let cursor = null;
  do {
    const data = await github.graphql(query, { owner, repo, cursor });
    const conn = data.repository.pullRequests;
    nodes.push(...conn.nodes);
    cursor = conn.pageInfo.hasNextPage ? conn.pageInfo.endCursor : null;
  } while (cursor);
  return nodes;
}

const FP_RE = /\*\*First Pass Reviewer:\s*@([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)\*\*/;
const MT_RE = /\*\*Maintainer Reviewer:\s*@([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)\*\*/;

function mapState(state) {
  switch (state) {
    case 'APPROVED': return 'Approved';
    case 'CHANGES_REQUESTED': return 'Changes req.';
    case 'COMMENTED': return 'Commented';
    default: return 'Pending';
  }
}
const statusEmoji = s =>
  s === 'Approved' ? '✅' : s === 'Changes req.' ? '🔴' : s === 'Commented' ? '💬' : '⏳';

// Turn a raw GraphQL PR node into the shape the renderer consumes.
function shape(node, firstPassPool, maintainerPool) {
  const author = node.author && node.author.login ? node.author.login : '(ghost)';

  let autoFP = null, autoMT = null;
  for (const c of (node.comments.nodes || [])) {
    const body = c.body || '';
    const fp = body.match(FP_RE);
    const mt = body.match(MT_RE);
    if (fp) autoFP = fp[1];
    if (mt) autoMT = mt[1];
  }

  const requested = (node.reviewRequests.nodes || [])
    .map(r => r.requestedReviewer && r.requestedReviewer.login)
    .filter(Boolean);

  const latest = {};
  for (const r of (node.reviews.nodes || [])) {
    const login = r.author && r.author.login;
    if (!login) continue;
    if (!latest[login] || (r.submittedAt || '') >= (latest[login].submittedAt || '')) latest[login] = r;
  }

  const universe = new Set([...requested, ...Object.keys(latest), autoFP, autoMT]
    .filter(Boolean).filter(l => l !== author));

  const firstPass = [], maintainers = [], other = [];
  for (const login of universe) {
    const via = (login === autoFP || login === autoMT) ? 'auto' : 'manual';
    const status = latest[login] ? mapState(latest[login].state) : 'Pending';
    const entry = { login, via, status };
    if (login === autoFP || (via === 'manual' && firstPassPool.has(login))) firstPass.push(entry);
    else if (login === autoMT || (via === 'manual' && maintainerPool.has(login))) maintainers.push(entry);
    else other.push(entry);
  }

  const hasAuto = !!(autoFP || autoMT);
  const hasManual = [...universe].some(l => l !== autoFP && l !== autoMT);
  let via = '—';
  if (hasAuto && hasManual) via = 'Mixed';
  else if (hasAuto) via = 'Auto';
  else if (hasManual) via = 'Manual';

  const daysIdle = Math.floor((Date.now() - new Date(node.updatedAt).getTime()) / 86400000);

  return {
    number: node.number, title: node.title, url: node.url, isDraft: node.isDraft,
    author, firstPass, maintainers, other, via,
    reviewerCount: universe.size, daysIdle, stale: daysIdle >= STALE_DAYS,
  };
}

// ── State: hidden JSON block in the issue body ───────────────────────────────
const STATE_OPEN = '<!--BOARD_STATE';
const STATE_CLOSE = 'BOARD_STATE-->';

function loadState(body) {
  const empty = { done: {}, notes: {}, priority: {}, claims: {}, log: [] };
  if (!body) return empty;
  const i = body.indexOf(STATE_OPEN);
  const j = body.indexOf(STATE_CLOSE);
  if (i === -1 || j === -1 || j < i) return empty;
  try {
    const json = body.slice(i + STATE_OPEN.length, j).trim();
    const parsed = JSON.parse(json);
    return Object.assign(empty, parsed);
  } catch (e) {
    return empty;
  }
}

// Parse one comment body for commands. Returns a list of {cmd, pr, arg, author}.
// Supported (one per line):
//   /done 1234        /undone 1234
//   /claim 1234       /unclaim 1234
//   /priority 1234 P2 (or just 2)
//   /note 1234 free text...   (empty text clears the note)
const CMD_RE = /^\s*\/(done|undone|claim|unclaim|priority|note)\s+#?(\d+)\b\s*(.*)$/i;

function parseCommands(commentBody, author) {
  const out = [];
  for (const line of String(commentBody || '').split('\n')) {
    const m = line.match(CMD_RE);
    if (!m) continue;
    out.push({ cmd: m[1].toLowerCase(), pr: Number(m[2]), arg: m[3].trim(), author });
  }
  return out;
}

function applyCommand(state, c, now) {
  const n = c.pr;
  let msg = null;
  switch (c.cmd) {
    case 'done': state.done[n] = true; msg = `@${c.author} marked #${n} done ✅`; break;
    case 'undone': state.done[n] = false; msg = `@${c.author} reopened #${n}`; break;
    case 'claim': {
      const arr = state.claims[n] || (state.claims[n] = []);
      if (!arr.includes(c.author)) arr.push(c.author);
      msg = `@${c.author} claimed #${n} 🙋`; break;
    }
    case 'unclaim': {
      state.claims[n] = (state.claims[n] || []).filter(l => l !== c.author);
      msg = `@${c.author} unclaimed #${n}`; break;
    }
    case 'priority': {
      const p = (c.arg.match(/[1-4]/) || [])[0];
      if (p) { state.priority[n] = Number(p); msg = `@${c.author} set #${n} → P${p}`; }
      else { state.priority[n] = null; msg = `@${c.author} cleared priority on #${n}`; }
      break;
    }
    case 'note':
      if (c.arg) { state.notes[n] = c.arg; msg = `@${c.author} noted #${n}: ${c.arg}`; }
      else { delete state.notes[n]; msg = `@${c.author} cleared note on #${n}`; }
      break;
  }
  if (msg) {
    state.log.unshift(`\`${now}\` — ${msg}`);
    state.log = state.log.slice(0, 12);
  }
  return msg;
}

// ── Rendering ────────────────────────────────────────────────────────────────
const esc = s => String(s == null ? '' : s).replace(/\|/g, '\\|').replace(/\r?\n/g, ' ');
const prLink = pr => `[#${pr.number}](${pr.url})`;
const badge = (label, msg, color) => {
  const enc = t => encodeURIComponent(String(t)).replace(/-/g, '--').replace(/_/g, '__').replace(/%20/g, '_');
  return `![${label}](https://img.shields.io/badge/${enc(label)}-${enc(msg)}-${color})`;
};

function reviewerCell(list) {
  if (!list.length) return '—';
  return list.map(r => `@${r.login} ${r.via === 'auto' ? '🤖' : '✋'} ${statusEmoji(r.status)}`).join('<br>');
}

function priorityOf(state, n) {
  if (state.priority[n] != null) return state.priority[n];
  return null;
}

function renderBody(prs, state, pools, now, targetLabel) {
  const L = [];
  const total = prs.length;
  const byPri = { 1: 0, 2: 0, 3: 0, 4: 0, none: 0 };
  let needsFP = 0, stale = 0, doneCount = 0;
  for (const pr of prs) {
    const p = priorityOf(state, pr.number);
    byPri[p == null ? 'none' : p]++;
    if (!pr.firstPass.length && !pr.isDraft) needsFP++;
    if (pr.stale) stale++;
    if (state.done[pr.number]) doneCount++;
  }

  L.push(`# 🗂️ Reviewer Triage Board`);
  L.push('');
  L.push([
    badge('open PRs', total, 'blue'),
    badge('P1', byPri[1], 'red'),
    badge('P2', byPri[2], 'orange'),
    badge('P3', byPri[3], 'yellow'),
    badge('needs first-pass', needsFP, needsFP ? 'critical' : 'green'),
    badge(`stale >${STALE_DAYS}d`, stale, stale ? 'lightgrey' : 'green'),
    badge('done', `${doneCount}/${total}`, 'brightgreen'),
  ].join(' '));
  L.push('');
  L.push(`_Auto-updated every ~5 min from **${targetLabel}** open PRs. Last run: **${now}**._`);
  L.push('');

  // Mermaid: PRs by priority.
  L.push('```mermaid');
  L.push('pie showData title Open PRs by priority');
  L.push(`    "P1 blocker" : ${byPri[1]}`);
  L.push(`    "P2" : ${byPri[2]}`);
  L.push(`    "P3" : ${byPri[3]}`);
  L.push(`    "P4 / unset" : ${byPri[4] + byPri.none}`);
  L.push('```');
  L.push('');

  // How to edit (the whole point: no write access needed).
  L.push('<details><summary>✍️ <b>How to update this board</b> (anyone — no write access needed)</summary>');
  L.push('');
  L.push('Add a **comment** with one or more of these commands. The bot applies it, then deletes your comment so this page stays fast:');
  L.push('');
  L.push('```');
  L.push('/done 1234           mark PR #1234 reviewed');
  L.push('/undone 1234         un-mark it');
  L.push('/claim 1234          put your name on it');
  L.push('/unclaim 1234        remove your name');
  L.push('/priority 1234 P2    set priority (P1–P4)');
  L.push('/note 1234 some text set a note (empty clears it)');
  L.push('```');
  L.push('</details>');
  L.push('');

  // Priority-grouped tables.
  const groups = [[1, 'P1 — launch blocker'], [2, 'P2'], [3, 'P3'], [4, 'P4 / unset']];
  const bucket = p => (p == null || p === 4) ? 4 : p;
  for (const [g, title] of groups) {
    const rows = prs.filter(pr => bucket(priorityOf(state, pr.number)) === g)
      .sort((a, b) => a.number - b.number);
    if (!rows.length) continue;
    L.push(`### ${title} · ${rows.length}`);
    L.push('');
    L.push('| PR | Title | Author | Via | First-pass | Maintainer | Claimed | Done | Notes |');
    L.push('|----|-------|--------|:---:|-----------|-----------|---------|:----:|-------|');
    for (const pr of rows) {
      const n = pr.number;
      const claimed = (state.claims[n] || []).map(l => `@${l}`).join(' ') || '—';
      const done = state.done[n] ? '✅' : '☐';
      const note = state.notes[n] ? esc(state.notes[n]) : '—';
      const title2 = esc(pr.title) + (pr.isDraft ? ' _(draft)_' : '') + (pr.stale ? ` ⏳${pr.daysIdle}d` : '');
      L.push(`| ${prLink(pr)} | ${title2} | @${pr.author} | ${pr.via} | ${reviewerCell(pr.firstPass)} | ${reviewerCell(pr.maintainers)} | ${claimed} | ${done} | ${note} |`);
    }
    L.push('');
  }

  // Per-reviewer collapsible queues (the "views").
  L.push('## 👥 Per-reviewer queues');
  L.push('');
  const reviewerPRs = new Map();
  for (const pr of prs) {
    for (const e of [...pr.firstPass, ...pr.maintainers, ...pr.other]) {
      if (!reviewerPRs.has(e.login)) reviewerPRs.set(e.login, []);
      reviewerPRs.get(e.login).push({ pr, e });
    }
  }
  const order = [...pools.firstPass, ...pools.maintainers,
    ...[...reviewerPRs.keys()].filter(l => !pools.firstPass.includes(l) && !pools.maintainers.includes(l)).sort()];
  const seen = new Set();
  for (const login of order) {
    if (seen.has(login)) continue; seen.add(login);
    const items = reviewerPRs.get(login);
    if (!items || !items.length) continue;
    const openCount = items.filter(it => !state.done[it.pr.number]).length;
    items.sort((a, b) => (bucket(priorityOf(state, a.pr.number))) - (bucket(priorityOf(state, b.pr.number))) || a.pr.number - b.pr.number);
    L.push(`<details><summary><b>@${login}</b> — ${items.length} PR(s), ${openCount} open</summary>`);
    L.push('');
    L.push('| PR | Title | Via | Status | Done |');
    L.push('|----|-------|:---:|:------:|:----:|');
    for (const { pr, e } of items) {
      L.push(`| ${prLink(pr)} | ${esc(pr.title)} | ${e.via === 'auto' ? '🤖' : '✋'} | ${statusEmoji(e.status)} ${e.status} | ${state.done[pr.number] ? '✅' : '☐'} |`);
    }
    L.push('');
    L.push('</details>');
  }
  L.push('');

  // Recent activity log.
  if (state.log && state.log.length) {
    L.push('<details><summary>🕓 <b>Recent activity</b></summary>');
    L.push('');
    for (const line of state.log) L.push(`- ${line}`);
    L.push('');
    L.push('</details>');
    L.push('');
  }

  L.push(`<sub>Priority: P1 launch blocker · P2–P3 nice to have · P4 likely not. Pools: ${pools.firstPass.length} first-pass, ${pools.maintainers.length} maintainers. Seeded once from the triage doc, maintained here via commands.</sub>`);
  L.push('');
  L.push(`${STATE_OPEN} ${JSON.stringify(state)} ${STATE_CLOSE}`);
  return L.join('\n');
}

// ── Orchestration ────────────────────────────────────────────────────────────
module.exports = async ({ github, context, core }) => {
  const { owner: hostOwner, repo: hostRepo } = context.repo; // where the issue lives + Action runs
  let readOwner = hostOwner, readRepo = hostRepo;
  if (process.env.TARGET_REPO && process.env.TARGET_REPO.includes('/')) {
    [readOwner, readRepo] = process.env.TARGET_REPO.split('/');
  }
  const issue_number = Number(process.env.DASHBOARD_ISSUE);
  if (!issue_number) throw new Error('DASHBOARD_ISSUE env var is required (the issue number to own).');
  const POOLS_PATH = process.env.POOLS_PATH || '.github/reviewer-pools.json';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 16) + ' UTC';

  let pools = { firstPass: [], maintainers: [] };
  try {
    const p = JSON.parse(fs.readFileSync(POOLS_PATH, 'utf8'));
    pools.firstPass = (p.firstPass || []).filter(Boolean);
    pools.maintainers = (p.maintainers || []).filter(Boolean);
  } catch (e) { core.warning(`pools: ${e.message}`); }
  const firstPassPool = new Set(pools.firstPass);
  const maintainerPool = new Set(pools.maintainers);

  // 1. PR data.
  const nodes = await gatherPRs(github, readOwner, readRepo);
  const prs = nodes.map(n => shape(n, firstPassPool, maintainerPool));
  core.info(`Gathered ${prs.length} open PRs from ${readOwner}/${readRepo}.`);

  // 2. Load prior state from the issue body.
  const issue = await github.rest.issues.get({ owner: hostOwner, repo: hostRepo, issue_number });
  const state = loadState(issue.data.body || '');

  // Seed priority for PRs the board has never recorded.
  for (const pr of prs) {
    if (!(pr.number in state.priority) && SEED_PRIORITY[pr.number] != null) {
      state.priority[pr.number] = SEED_PRIORITY[pr.number];
    }
  }

  // 3. Process command comments, then delete them.
  const comments = await github.paginate(github.rest.issues.listComments,
    { owner: hostOwner, repo: hostRepo, issue_number, per_page: 100 });
  let processed = 0;
  for (const c of comments) {
    const author = c.user && c.user.login;
    if (author === 'github-actions[bot]') continue; // never touch our own log comments
    const cmds = parseCommands(c.body, author);
    if (!cmds.length) continue;
    for (const cmd of cmds) applyCommand(state, cmd, now);
    try {
      await github.rest.issues.deleteComment({ owner: hostOwner, repo: hostRepo, comment_id: c.id });
      processed++;
    } catch (e) {
      core.warning(`Could not delete comment ${c.id}: ${e.message}`);
    }
  }
  if (processed) core.info(`Applied and cleared ${processed} command comment(s).`);

  // 4. Rewrite the issue body.
  const targetLabel = `${readOwner}/${readRepo}`;
  const body = renderBody(prs, state, pools, now, targetLabel);
  if ((issue.data.body || '') !== body) {
    await github.rest.issues.update({ owner: hostOwner, repo: hostRepo, issue_number, body });
    core.info('Board updated.');
  } else {
    core.info('No change.');
  }
};

// Exposed for offline testing.
module.exports._internal = {
  shape, loadState, parseCommands, applyCommand, renderBody, SEED_PRIORITY,
};
