const { chromium } = require('playwright');

(async () => {
  const url = process.argv[2] || 'http://127.0.0.1:5003';
  const outFile = process.argv[3] || 'dashboard-check.png';

  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
  await page.goto(url, { waitUntil: 'networkidle' });
  await page.screenshot({ path: outFile, fullPage: true });
  await browser.close();
  console.log(`Saved screenshot to ${outFile}`);
})();
