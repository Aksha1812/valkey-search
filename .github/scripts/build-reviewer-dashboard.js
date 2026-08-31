// Reviewer dashboard builder.
//
// Regenerates the "Reviewer-Dashboard" wiki pages from live GitHub state while
// preserving the three human-owned columns on the master page — Priority, Done,
// and Notes — by parsing the previous copy of the page and re-injecting them,
// keyed by PR number. Everything else (title, author, reviewers, review state,
// auto-vs-manual) is overwritten from GitHub each run.
//
// Invoked from a github-script step:
//   const build = require('./.github/scripts/build-reviewer-dashboard.js');
//   await build({ github, context, core });
//
// It reads .github/reviewer-pools.json from the checked-out repo, writes the
// rendered pages into $WIKI_DIR (default "wiki"), and leaves committing/pushing
// to the workflow.

const fs = require('fs');

// Page file names in the wiki repo. Only these are ever written; other wiki
// pages (Home, _Sidebar, …) are left untouched so this is safe on a shared wiki.
const PAGES = {
  master: 'Reviewer-Dashboard.md',
  firstPass: 'Reviewer-Dashboard-First-Pass.md',
  maintainers: 'Reviewer-Dashboard-Maintainers.md',
  auto: 'Reviewer-Dashboard-Auto-Assigned.md',
  manual: 'Reviewer-Dashboard-Manually-Assigned.md',
};

// One-time priority seed, transcribed from Allen's triage doc. Only used for a
// PR the master page has never listed before; once a PR appears on the page its
// wiki value wins (including a human clearing it), so the doc is never re-read.
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

// ── GitHub data gathering (GraphQL, a handful of calls per run) ──────────────
async function gather(github, owner, repo) {
  const query = `
    query($owner:String!, $repo:String!, $cursor:String) {
      repository(owner:$owner, name:$repo) {
        pullRequests(states:OPEN, first:25, after:$cursor, orderBy:{field:CREATED_AT, direction:ASC}) {
          pageInfo { hasNextPage endCursor }
          nodes {
            number title url isDraft createdAt
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

// Turn a raw GraphQL PR node into the shape the renderer consumes.
function shape(node, firstPassPool, maintainerPool) {
  const author = node.author && node.author.login ? node.author.login : '(ghost)';

  // Auto picks: last comment that carries the assignment signature wins
  // (a later /assign-reviewers re-run supersedes the original).
  let autoFP = null, autoMT = null;
  for (const c of (node.comments.nodes || [])) {
    const body = c.body || '';
    const fp = body.match(FP_RE);
    const mt = body.match(MT_RE);
    if (fp || mt) {
      if (fp) autoFP = fp[1];
      if (mt) autoMT = mt[1];
    }
  }

  const requested = (node.reviewRequests.nodes || [])
    .map(r => r.requestedReviewer && r.requestedReviewer.login)
    .filter(Boolean);

  // Latest review state per login.
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

  return {
    number: node.number,
    title: node.title,
    url: node.url,
    isDraft: node.isDraft,
    author,
    firstPass, maintainers, other,
    via,
  };
}

// ── Human-state preservation ─────────────────────────────────────────────────
// Parse the previous master page for the human-owned cells, keyed by PR number.
// Returns { present:Set<number>, priority:{}, done:{}, notes:{} }.
function parseHumanState(markdown) {
  const out = { present: new Set(), priority: {}, done: {}, notes: {} };
  if (!markdown) return out;
  for (const line of markdown.split('\n')) {
    const t = line.trim();
    if (!t.startsWith('|')) continue;
    // Split on unescaped pipes only — Notes/Title cells escape their own pipes
    // as "\|", so a naive split on every "|" would miscount columns.
    const cells = t.split(/(?<!\\)\|/).slice(1, -1).map(c => c.trim());
    // master columns: PR|Title|Author|Priority|Via|First-pass|Maintainers|Done|Notes
    if (cells.length < 9) continue;
    const m = cells[0].match(/#(\d+)/);
    if (!m) continue; // header / separator / non-data row
    const n = Number(m[1]);
    out.present.add(n);
    const pri = (cells[3].match(/[1-4]/) || [''])[0];
    out.priority[n] = pri ? Number(pri) : null;
    out.done[n] = /\[x\]/i.test(cells[7]);
    out.notes[n] = cells[8] === '—' ? '' : cells[8].replace(/\\\|/g, '|');
  }
  return out;
}

// ── Rendering ────────────────────────────────────────────────────────────────
const esc = s => String(s == null ? '' : s).replace(/\|/g, '\\|');
const prLink = pr => `[#${pr.number}](${pr.url})`;
const viaTag = v => (v === 'auto' ? '🤖 auto' : '✋ manual');

function reviewerCell(list) {
  if (!list.length) return '—';
  return list.map(r => `@${r.login} · ${viaTag(r.via)} · ${r.status}`).join('<br>');
}

// Sort: by priority (1 first, unset last), then oldest PR first.
function bySort(a, b, priority) {
  const pa = priority[a.number] || 99, pb = priority[b.number] || 99;
  if (pa !== pb) return pa - pb;
  return a.number - b.number;
}

function renderMaster(prs, state, now, counts) {
  const rows = prs.slice().sort((a, b) => bySort(a, b, state.priority));
  const lines = [];
  lines.push('# Reviewer Dashboard');
  lines.push('');
  lines.push(`_Auto-updated every ~5 min from open PRs. Last run: **${now}**. Open PRs: **${prs.length}**._`);
  lines.push('');
  lines.push('**Views:** ' + [
    `[First-pass reviewers](${PAGES.firstPass.replace('.md', '')})`,
    `[Maintainers](${PAGES.maintainers.replace('.md', '')})`,
    `[Auto-assigned](${PAGES.auto.replace('.md', '')})`,
    `[Manually assigned](${PAGES.manual.replace('.md', '')})`,
  ].join(' · '));
  lines.push('');
  lines.push('> **Editable columns:** `Priority` (1 = launch blocker … 4 = likely not), `Done`, and `Notes` are yours — edit them in the page source and the updater preserves them. Everything else is regenerated from GitHub, so don\'t bother editing it. To toggle Done, change `[ ]` to `[x]`.');
  lines.push('');
  lines.push('| PR | Title | Author | Priority | Via | First-pass reviewer(s) | Maintainer(s) | Done | Notes |');
  lines.push('|----|-------|--------|:--------:|:---:|------------------------|---------------|:----:|-------|');
  for (const pr of rows) {
    const n = pr.number;
    const pri = state.priority[n] != null ? state.priority[n] : '';
    const done = state.done[n] ? '[x]' : '[ ]';
    const notes = state.notes[n] ? esc(state.notes[n]) : '—';
    const title = esc(pr.title) + (pr.isDraft ? ' _(draft)_' : '');
    lines.push(`| ${prLink(pr)} | ${title} | @${pr.author} | ${pri} | ${pr.via} | ${reviewerCell(pr.firstPass)} | ${reviewerCell(pr.maintainers)} | ${done} | ${notes} |`);
  }
  lines.push('');
  lines.push(`<sub>Priority legend: **1** launch blocker · **2–3** nice to have · **4** likely not. Seeded once from the triage doc, maintained here. Pools: ${counts.fp} first-pass, ${counts.mt} maintainers.</sub>`);
  return lines.join('\n') + '\n';
}

// A per-reviewer view: group PRs by the reviewer login, sorted by priority.
function renderByReviewer(prs, state, now, pool, side, title, blurb) {
  const groups = new Map(); // login -> [{pr, entry}]
  for (const pr of prs) {
    for (const entry of pr[side]) {
      if (!groups.has(entry.login)) groups.set(entry.login, []);
      groups.get(entry.login).push({ pr, entry });
    }
  }
  const lines = [`# ${title}`, '', `_${blurb} Last run: **${now}**._`, '', `[← Back to dashboard](${PAGES.master.replace('.md', '')})`, ''];
  // Pool members first (in pool order), then any extra reviewers seen.
  const ordered = [...pool, ...[...groups.keys()].filter(l => !pool.includes(l)).sort()];
  let any = false;
  for (const login of ordered) {
    const items = groups.get(login);
    if (!items || !items.length) continue;
    any = true;
    items.sort((x, y) => bySort(x.pr, y.pr, state.priority));
    lines.push(`## @${login} — ${items.length} PR${items.length === 1 ? '' : 's'}`);
    lines.push('');
    lines.push('| PR | Title | Author | Priority | Via | Status | Done | Notes |');
    lines.push('|----|-------|--------|:--------:|:---:|:------:|:----:|-------|');
    for (const { pr, entry } of items) {
      const pri = state.priority[pr.number] != null ? state.priority[pr.number] : '';
      const done = state.done[pr.number] ? '✅' : '☐';
      const notes = state.notes[pr.number] ? esc(state.notes[pr.number]) : '—';
      lines.push(`| ${prLink(pr)} | ${esc(pr.title)} | @${pr.author} | ${pri} | ${viaTag(entry.via)} | ${entry.status} | ${done} | ${notes} |`);
    }
    lines.push('');
  }
  if (!any) lines.push('_No PRs currently assigned._');
  return lines.join('\n') + '\n';
}

// Auto / manual filter views: one flat table of (PR, reviewer) pairs matching
// the requested assignment kind.
function renderByVia(prs, state, now, wantVia, title, blurb) {
  const rows = [];
  for (const pr of prs) {
    for (const side of ['firstPass', 'maintainers', 'other']) {
      for (const entry of pr[side]) {
        if (entry.via === wantVia) rows.push({ pr, entry, role: side });
      }
    }
  }
  rows.sort((a, b) => bySort(a.pr, b.pr, state.priority) || a.pr.number - b.pr.number);
  const roleLabel = { firstPass: 'first-pass', maintainers: 'maintainer', other: 'other' };
  const lines = [`# ${title}`, '', `_${blurb} Last run: **${now}**._`, '', `[← Back to dashboard](${PAGES.master.replace('.md', '')})`, ''];
  lines.push('| PR | Title | Author | Priority | Reviewer | Role | Status | Done |');
  lines.push('|----|-------|--------|:--------:|----------|:----:|:------:|:----:|');
  for (const { pr, entry, role } of rows) {
    const pri = state.priority[pr.number] != null ? state.priority[pr.number] : '';
    const done = state.done[pr.number] ? '✅' : '☐';
    lines.push(`| ${prLink(pr)} | ${esc(pr.title)} | @${pr.author} | ${pri} | @${entry.login} | ${roleLabel[role]} | ${entry.status} | ${done} |`);
  }
  if (!rows.length) lines.push('| _none_ |  |  |  |  |  |  |  |');
  return lines.join('\n') + '\n';
}

function renderAll(prs, state, now, pools) {
  const counts = { fp: pools.firstPass.length, mt: pools.maintainers.length };
  return {
    [PAGES.master]: renderMaster(prs, state, now, counts),
    [PAGES.firstPass]: renderByReviewer(prs, state, now, pools.firstPass, 'firstPass',
      'First-pass reviewers', 'PRs grouped by their first-pass reviewer, sorted by priority.'),
    [PAGES.maintainers]: renderByReviewer(prs, state, now, pools.maintainers, 'maintainers',
      'Maintainers', 'PRs grouped by their maintainer reviewer, sorted by priority.'),
    [PAGES.auto]: renderByVia(prs, state, now, 'auto',
      'Auto-assigned reviews', 'Every reviewer that the auto-assign workflow picked, sorted by priority.'),
    [PAGES.manual]: renderByVia(prs, state, now, 'manual',
      'Manually-assigned reviews', 'Reviewers added by hand (not by the auto-assign workflow), sorted by priority.'),
  };
}

// ── Orchestration ────────────────────────────────────────────────────────────
module.exports = async ({ github, context, core }) => {
  // By default read the PRs of the repo the workflow runs in. TARGET_REPO
  // ("owner/name") overrides the *read* source only — useful to demo on a fork
  // while reading a busier upstream repo; the wiki written is still this repo's.
  let { owner, repo } = context.repo;
  if (process.env.TARGET_REPO && process.env.TARGET_REPO.includes('/')) {
    [owner, repo] = process.env.TARGET_REPO.split('/');
    core.info(`Reading PRs from ${owner}/${repo} (TARGET_REPO override).`);
  }
  const WIKI_DIR = process.env.WIKI_DIR || 'wiki';
  const POOLS_PATH = process.env.POOLS_PATH || '.github/reviewer-pools.json';
  const now = new Date().toISOString().replace('T', ' ').slice(0, 16) + ' UTC';

  let pools = { firstPass: [], maintainers: [] };
  try {
    pools = JSON.parse(fs.readFileSync(POOLS_PATH, 'utf8'));
    pools.firstPass = (pools.firstPass || []).filter(Boolean);
    pools.maintainers = (pools.maintainers || []).filter(Boolean);
  } catch (e) {
    core.warning(`Could not read ${POOLS_PATH}: ${e.message}`);
    pools.firstPass = pools.firstPass || [];
    pools.maintainers = pools.maintainers || [];
  }
  const firstPassPool = new Set(pools.firstPass);
  const maintainerPool = new Set(pools.maintainers);

  const nodes = await gather(github, owner, repo);
  const prs = nodes.map(n => shape(n, firstPassPool, maintainerPool));
  core.info(`Gathered ${prs.length} open PRs.`);

  // Merge human state from the previous master page.
  let prev = '';
  try { prev = fs.readFileSync(`${WIKI_DIR}/${PAGES.master}`, 'utf8'); } catch (e) { /* first run */ }
  const human = parseHumanState(prev);
  const state = { priority: {}, done: {}, notes: {} };
  for (const pr of prs) {
    const n = pr.number;
    if (human.present.has(n)) {
      state.priority[n] = human.priority[n];      // wiki wins (incl. blank)
      state.done[n] = human.done[n];
      state.notes[n] = human.notes[n];
    } else {
      state.priority[n] = SEED_PRIORITY[n] != null ? SEED_PRIORITY[n] : null; // seed only new PRs
      state.done[n] = false;
      state.notes[n] = '';
    }
  }

  const pages = renderAll(prs, state, now, pools);
  if (!fs.existsSync(WIKI_DIR)) fs.mkdirSync(WIKI_DIR, { recursive: true });
  for (const [name, content] of Object.entries(pages)) {
    fs.writeFileSync(`${WIKI_DIR}/${name}`, content);
    core.info(`Wrote ${name} (${content.length} bytes)`);
  }
};

// Exposed for local preview/testing without the GitHub API.
module.exports._internal = { shape, parseHumanState, renderAll, SEED_PRIORITY, PAGES };
