// 用 ts compile 产出规范 Core IR v2 字节
import { readFileSync, writeFileSync } from 'node:fs';
const [,, tsRoot, src, out] = process.argv;
const { compile } = await import(tsRoot + '/dist/src/browser.js');
const r = compile(readFileSync(src, 'utf8'));
if (!r.success) { console.error(JSON.stringify(r)); process.exit(1); }
writeFileSync(out, r.coreJson);
console.log(out, Buffer.byteLength(r.coreJson));
