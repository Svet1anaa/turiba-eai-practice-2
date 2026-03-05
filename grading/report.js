/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');

const TEST_CASES = [
  { idx: 1, title: 'Services start without errors', points: 3 },
  { idx: 2, title: 'POST /orders returns correlationId', points: 4 },
  { idx: 3, title: 'Exchange + queue topology correct', points: 3 },
  { idx: 4, title: 'DLQ receives failed messages', points: 4 },
  { idx: 5, title: 'Correlation ID propagated', points: 3 },
  { idx: 6, title: 'Idempotent notification', points: 4 },
  { idx: 7, title: 'Retry logic (3 attempts)', points: 2 }
];

function loadResults(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }

  const raw = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(raw);
}

function flattenAssertions(testResults) {
  return testResults.flatMap((suite) => suite.assertionResults || []);
}

function scoreFromResults(results) {
  const assertions = flattenAssertions(results.testResults || []);

  const rows = TEST_CASES.map((testCase) => {
    const match = assertions.find(
      (a) =>
        a.title?.startsWith(`${testCase.idx})`) ||
        a.fullName?.includes(` ${testCase.idx})`) ||
        a.fullName?.startsWith(`${testCase.idx})`)
    );
    if (!match) {
      return {
        ...testCase,
        awarded: 0,
        status: 'FAIL: Test not found'
      };
    }

    if (match.status === 'passed') {
      return {
        ...testCase,
        awarded: testCase.points,
        status: 'PASS'
      };
    }

    const message = (match.failureMessages || [])[0] || 'Unknown failure';
    const cleaned = message
      .replace(/\u001b\[[0-9;]*m/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 180);

    return {
      ...testCase,
      awarded: 0,
      status: `FAIL: ${cleaned}`
    };
  });

  const automated = rows.reduce((sum, r) => sum + r.awarded, 0);
  return { rows, automated };
}

function formatReport({ commit, timestamp, rows, automated }) {
  const lines = [];
  lines.push('# Grade Report: Practice 2 — Event-Driven Messaging');
  lines.push('');
  lines.push(`**Submission:** ${timestamp}`);
  lines.push(`**Commit:** ${commit}`);
  lines.push('');
  lines.push('## Results');
  lines.push('');
  lines.push('| # | Test | Points | Status |');
  lines.push('|---|---|---|---|');

  for (const row of rows) {
    lines.push(`| ${row.idx} | ${row.title} | ${row.awarded}/${row.points} | ${row.status} |`);
  }

  lines.push('| 8 | Code quality + README | -/2 | MANUAL REVIEW |');
  lines.push('');
  lines.push(`**Automated Score: ${automated}/23**`);
  lines.push('**Manual Review Needed: 2 points**');
  lines.push('');

  return lines.join('\n');
}

function nowUtc() {
  return new Date().toISOString().replace('T', ' ').replace('.000Z', ' UTC').replace('Z', ' UTC');
}

async function main() {
  const inputArg = process.argv[2] || path.join('test', 'results.json');
  const inputPath = path.resolve(process.cwd(), inputArg);
  const outputPath = path.resolve(process.cwd(), 'grade-report.md');

  const commit = process.env.GITHUB_SHA ? process.env.GITHUB_SHA.slice(0, 7) : 'local';
  const timestamp = nowUtc();

  const parsed = loadResults(inputPath);

  let report;
  if (!parsed) {
    report = [
      '# Grade Report: Practice 2 — Event-Driven Messaging',
      '',
      `**Submission:** ${timestamp}`,
      `**Commit:** ${commit}`,
      '',
      '## Results',
      '',
      '| # | Test | Points | Status |',
      '|---|---|---|---|',
      ...TEST_CASES.map((t) => `| ${t.idx} | ${t.title} | 0/${t.points} | FAIL: results.json not found |`),
      '| 8 | Code quality + README | -/2 | MANUAL REVIEW |',
      '',
      '**Automated Score: 0/23**',
      '**Manual Review Needed: 2 points**'
    ].join('\n');
  } else {
    const scored = scoreFromResults(parsed);
    report = formatReport({
      commit,
      timestamp,
      rows: scored.rows,
      automated: scored.automated
    });
  }

  fs.writeFileSync(outputPath, report, 'utf8');
  console.log(`Grade report generated at ${outputPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

