package aster.cli;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * 读包（README「包路径」「zip 规则」「体积上限」；参照实现 {@code reference-check.mjs} 的 {@code readDirectory} / {@code readZip}）。
 *
 * <p>路径指向目录即目录形态、指向普通文件即 zip，与扩展名无关。zip 只在内存解压、从不写盘：整个文件读入后同时解析中央目录与本地头
 * 并要求二者一致（{@code java.util.zip.ZipInputStream} 只看本地头、{@code ZipFile} 只看中央目录，单用任何一个都会放过夹具
 * {@code zip-lfh-*}），deflate 边解压边计数，并要求压缩数据恰好是一个完整的 deflate 流（{@link Inflater#getBytesRead()} = csize）。
 * 失败一律 {@link ErrorCode#PKG_INTEGRITY}。
 */
public final class PackageReader {

    public static final long MAX_PACKAGE_BYTES = 64L * 1024 * 1024;
    private static final Pattern PATH = Pattern.compile("[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*");
    private static final int CHUNK = 64 * 1024;

    private PackageReader() {}

    /** 读包，返回「包内相对路径 → 字节」。 */
    public static Map<String, byte[]> read(Path path) throws CliException {
        BasicFileAttributes attrs;
        try {
            attrs = Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            throw fail("包路径不存在或不可读: " + path);
        }
        if (attrs.isDirectory()) {
            return readDirectory(path);
        }
        if (!attrs.isRegularFile()) {
            throw fail("既不是目录也不是普通文件: " + path);
        }
        if (attrs.size() > MAX_PACKAGE_BYTES) {
            throw fail("zip 超过 64 MiB");
        }
        try {
            return readZip(Files.readAllBytes(path));
        } catch (IOException e) {
            throw fail("读取 zip 失败: " + e.getMessage());
        }
    }

    /** 包内相对路径规则（{@code files[].path} 与 zip 去掉根之后的条目名共用）。 */
    public static boolean isPackagePath(String p) {
        if (!PATH.matcher(p).matches()) {
            return false;
        }
        for (String seg : p.split("/", -1)) {
            if (seg.equals(".") || seg.equals("..")) {
                return false;
            }
        }
        return true;
    }

    private static CliException fail(String why) {
        return new CliException(ErrorCode.PKG_INTEGRITY, why);
    }

    // ── 目录形态 ─────────────────────────────────────────────────

    private static Map<String, byte[]> readDirectory(Path root) throws CliException {
        Map<String, byte[]> files = new LinkedHashMap<>();
        long[] total = {0};
        walk(root, "", files, total);
        return files;
    }

    private static void walk(Path dir, String rel, Map<String, byte[]> files, long[] total) throws CliException {
        List<Path> children;
        try (Stream<Path> s = Files.list(dir)) {
            children = s.sorted().toList();
        } catch (IOException e) {
            throw fail("无法列出目录 " + rel);
        }
        for (Path child : children) {
            String name = child.getFileName().toString();
            String r = rel.isEmpty() ? name : rel + "/" + name;
            BasicFileAttributes a;
            try {
                a = Files.readAttributes(child, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            } catch (IOException e) {
                throw fail("无法读取 " + r);
            }
            if (a.isSymbolicLink()) {
                throw fail("符号链接 " + r);
            }
            if (a.isDirectory()) {
                walk(child, r, files, total);
                continue;
            }
            if (!a.isRegularFile()) {
                throw fail("不是普通文件 " + r);
            }
            // 先按 stat 的大小判上限，不先读入再判断
            if (a.size() > MAX_PACKAGE_BYTES || total[0] + a.size() > MAX_PACKAGE_BYTES) {
                throw fail("目录形态超过 64 MiB");
            }
            total[0] += a.size();
            try {
                files.put(r, Files.readAllBytes(child));
            } catch (IOException e) {
                throw fail("无法读取 " + r);
            }
        }
    }

    // ── zip 形态 ─────────────────────────────────────────────────

    private record CentralEntry(int flags, int method, long crc, long csize, long usize, byte[] name, byte[] extra,
                                long offset) {}

    static Map<String, byte[]> readZip(byte[] bytes) throws CliException {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int len = bytes.length;
        if (len > MAX_PACKAGE_BYTES) {
            throw fail("zip 超过 64 MiB");
        }
        if (len < 22 || u32(buf, len - 22) != 0x06054b50L) {
            throw fail("EOCD 不在文件末尾（有注释、尾随字节或不是 zip）");
        }
        int e = len - 22;
        int count = u16(buf, e + 10);
        long cdSize = u32(buf, e + 12);
        long cdOff = u32(buf, e + 16);
        if (u16(buf, e + 4) != 0 || u16(buf, e + 6) != 0 || u16(buf, e + 8) != count) {
            throw fail("多卷 zip");
        }
        if (count == 0xffff || cdSize == 0xffffffffL || cdOff == 0xffffffffL) {
            throw fail("ZIP64");
        }
        if (cdOff + cdSize != e) {
            throw fail("中央目录与 EOCD 之间有间隙");
        }
        if (count == 0) {
            throw fail("空 zip");
        }
        List<CentralEntry> cds = readCentralDirectory(buf, (int) cdOff, e, count);
        return readEntries(buf, cds, (int) cdOff);
    }

    private static List<CentralEntry> readCentralDirectory(ByteBuffer buf, int cdOff, int eocd, int count)
            throws CliException {
        List<CentralEntry> cds = new ArrayList<>(count);
        int p = cdOff;
        for (int k = 0; k < count; k++) {
            if (p + 46 > eocd || u32(buf, p) != 0x02014b50L) {
                throw fail("中央目录损坏");
            }
            int nameLen = u16(buf, p + 28);
            int extraLen = u16(buf, p + 30);
            int commentLen = u16(buf, p + 32);
            if (p + 46 + nameLen + extraLen + commentLen > eocd) {
                throw fail("中央目录损坏");
            }
            cds.add(new CentralEntry(u16(buf, p + 8), u16(buf, p + 10), u32(buf, p + 16), u32(buf, p + 20),
                u32(buf, p + 24), slice(buf, p + 46, nameLen), slice(buf, p + 46 + nameLen, extraLen),
                u32(buf, p + 42)));
            if (commentLen != 0) {
                throw fail("条目注释");
            }
            p += 46 + nameLen + extraLen + commentLen;
        }
        if (p != eocd) {
            throw fail("中央目录长度不符");
        }
        return cds;
    }

    private static Map<String, byte[]> readEntries(ByteBuffer buf, List<CentralEntry> cds, int cdOff)
            throws CliException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        Set<String> seen = new HashSet<>();
        long budget = MAX_PACKAGE_BYTES;
        String root = null;
        long at = 0;
        for (CentralEntry cd : cds) {
            if (cd.offset() != at || at + 30 > cdOff || u32(buf, (int) at) != 0x04034b50L) {
                throw fail("本地头位置与中央目录不符（或有前置字节）");
            }
            int a = (int) at;
            int flags = u16(buf, a + 6);
            int method = u16(buf, a + 8);
            int nameLen = u16(buf, a + 26);
            int extraLen = u16(buf, a + 28);
            if ((long) a + 30 + nameLen + extraLen > cdOff) {
                throw fail("本地头越过中央目录");
            }
            byte[] nameBytes = slice(buf, a + 30, nameLen);
            byte[] extra = slice(buf, a + 30 + nameLen, extraLen);
            if (flags != cd.flags() || method != cd.method() || !Arrays.equals(nameBytes, cd.name())) {
                throw fail("本地头与中央目录不一致（标志 / 方法 / 名字）");
            }
            if ((flags & ~0x0808) != 0) {
                throw fail("不支持的通用标志位（加密等）");
            }
            if (hasZip64Extra(extra) || hasZip64Extra(cd.extra())) {
                throw fail("ZIP64 extra");
            }
            if (cd.csize() == 0xffffffffL || cd.usize() == 0xffffffffL) {
                throw fail("ZIP64 大小");
            }
            if (method != 0 && method != 8) {
                throw fail("不支持的压缩方法 " + method);
            }
            boolean descriptor = (flags & 0x0008) != 0;
            if (descriptor && method != 8) {
                throw fail("存储条目不得使用数据描述符");
            }
            long lcrc = u32(buf, a + 14);
            long lcs = u32(buf, a + 18);
            long lus = u32(buf, a + 22);
            boolean headerOk = descriptor
                ? lcrc == 0 && lcs == 0 && lus == 0
                : lcrc == cd.crc() && lcs == cd.csize() && lus == cd.usize();
            if (!headerOk) {
                throw fail("本地头与中央目录不一致（crc / 大小）");
            }
            long dataAt = (long) a + 30 + nameLen + extraLen;
            long next = dataAt + cd.csize();
            if (descriptor) {
                if (next + 16 > cdOff || u32(buf, (int) next) != 0x08074b50L) {
                    throw fail("缺少带签名的数据描述符");
                }
                int d = (int) next;
                if (u32(buf, d + 4) != cd.crc() || u32(buf, d + 8) != cd.csize() || u32(buf, d + 12) != cd.usize()) {
                    throw fail("数据描述符与中央目录不一致");
                }
                next += 16;
            }
            if (next > cdOff) {
                throw fail("条目越过中央目录");
            }
            String name = decodeName(nameBytes, flags);
            int slash = name.indexOf('/');
            String r = slash < 0 ? name : name.substring(0, slash);
            if (root == null) {
                root = r;
            }
            if (!r.equals(root) || slash < 0 || !isSafeRoot(r)) {
                throw fail("条目不在唯一的安全根目录下: " + name);
            }
            String rest = name.substring(slash + 1);
            boolean isDir = rest.isEmpty() || rest.endsWith("/");
            String rel = isDir && !rest.isEmpty() ? rest.substring(0, rest.length() - 1) : rest;
            // 根目录条目只能是 "<root>/"；"<root>//" 之类去掉斜杠后为空的写法不是合法路径
            if (rest.isEmpty() ? !isDir : rel.isEmpty() || !isPackagePath(rel)) {
                throw fail("条目路径不合法: " + name);
            }
            if (!seen.add(name)) {
                throw fail("重复条目 " + name);
            }
            if (isDir && cd.usize() != 0) {
                throw fail("目录条目带数据");
            }
            // 目录条目同样解压并校验大小与 CRC（README「zip 规则」逐条目适用），只是不放进结果
            byte[] data = method == 0
                ? slice(buf, (int) dataAt, (int) cd.csize())
                : inflate(buf, (int) dataAt, (int) cd.csize(), Math.min(cd.usize(), budget), name);
            if (data.length != cd.usize()) {
                throw fail("实际大小与声明不符: " + name);
            }
            if (data.length > budget) {
                throw fail("累计解压超过 64 MiB");
            }
            budget -= data.length;
            CRC32 crc = new CRC32();
            crc.update(data);
            if (crc.getValue() != cd.crc()) {
                throw fail("CRC 不符: " + name);
            }
            if (!isDir) {
                entries.put(rel, data);
            }
            at = next;
        }
        if (at != cdOff) {
            throw fail("最后一个条目与中央目录之间有多余字节");
        }
        return entries;
    }

    /**
     * 解压一个 deflate 条目，至多产出 {@code limit + 1} 字节（超出即失败，不再继续解压——解压炸弹在此截住）。
     * 压缩数据必须恰好是一个完整的 deflate 流：结束时已消耗字节数须等于 csize。
     */
    private static byte[] inflate(ByteBuffer buf, int at, int csize, long limit, String name) throws CliException {
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(slice(buf, at, csize));
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream((int) Math.min(limit, CHUNK));
            byte[] chunk = new byte[CHUNK];
            while (!inflater.finished()) {
                int n = inflater.inflate(chunk);
                if (n == 0 && (inflater.needsInput() || inflater.needsDictionary())) {
                    throw fail("deflate 流不完整: " + name);
                }
                out.write(chunk, 0, n);
                if (out.size() > limit) {
                    throw fail("解压超过声明大小 / 64 MiB: " + name);
                }
            }
            if (inflater.getBytesRead() != csize) {
                throw fail("deflate 流之后有多余字节: " + name);
            }
            return out.toByteArray();
        } catch (DataFormatException e) {
            throw fail("解压失败: " + name);
        } finally {
            inflater.end();
        }
    }

    private static String decodeName(byte[] bytes, int flags) throws CliException {
        String s;
        try {
            s = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw fail("条目名不是合法 UTF-8");
        }
        boolean nonAscii = s.chars().anyMatch(c -> c > 0x7f);
        if (nonAscii && (flags & 0x0800) == 0) {
            throw fail("非 ASCII 条目名未置 UTF-8 标志");
        }
        return s;
    }

    /** zip 根目录段：非空、NFC、不是 . / ..、不含 / \ 与控制字符（C0、DEL、C1）。 */
    static boolean isSafeRoot(String r) {
        if (r.isEmpty() || r.equals(".") || r.equals("..") || !Normalizer.isNormalized(r, Normalizer.Form.NFC)) {
            return false;
        }
        return r.chars().noneMatch(c -> c == '/' || c == '\\' || c <= 0x1f || (c >= 0x7f && c <= 0x9f));
    }

    private static boolean hasZip64Extra(byte[] extra) {
        ByteBuffer x = ByteBuffer.wrap(extra).order(ByteOrder.LITTLE_ENDIAN);
        for (int k = 0; k + 4 <= extra.length; k += 4 + u16(x, k + 2)) {
            if (u16(x, k) == 0x0001) {
                return true;
            }
        }
        return false;
    }

    private static int u16(ByteBuffer b, int at) {
        return b.getShort(at) & 0xffff;
    }

    private static long u32(ByteBuffer b, int at) {
        return b.getInt(at) & 0xffffffffL;
    }

    private static byte[] slice(ByteBuffer b, int at, int len) {
        byte[] out = new byte[len];
        b.get(at, out);
        return out;
    }
}
