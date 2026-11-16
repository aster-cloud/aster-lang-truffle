package aster.cli;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** zip 目录条目的规则（README「zip 规则」）：夹具未覆盖的边角。 */
class PackageReaderTest {

    /** 用 JDK ZipOutputStream 写出（deflate 条目带数据描述符，与 aster-api 导出同形）。 */
    private static byte[] zip(Object... nameAndData) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream z = new ZipOutputStream(bytes)) {
            for (int k = 0; k < nameAndData.length; k += 2) {
                String name = (String) nameAndData[k];
                byte[] data = ((String) nameAndData[k + 1]).getBytes(StandardCharsets.UTF_8);
                ZipEntry e = new ZipEntry(name);
                e.setTime(315532800000L);
                if (name.endsWith("/")) {
                    // 目录条目只能是存储方法；带数据须事先给出大小与 CRC
                    CRC32 crc = new CRC32();
                    crc.update(data);
                    e.setMethod(ZipEntry.STORED);
                    e.setSize(data.length);
                    e.setCompressedSize(data.length);
                    e.setCrc(crc.getValue());
                }
                z.putNextEntry(e);
                z.write(data);
                z.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    @Test
    void readsRootAndNestedDirectoryEntries() throws Exception {
        Map<String, byte[]> files = PackageReader.readZip(zip("pkg/", "", "pkg/policy/", "", "pkg/policy/a.txt", "hi"));
        assertEquals(1, files.size());
        assertArrayEquals("hi".getBytes(StandardCharsets.UTF_8), files.get("policy/a.txt"));
    }

    @Test
    void rejectsEmptyPathUnderRoot() throws Exception {
        CliException e = assertThrows(CliException.class, () -> PackageReader.readZip(zip("pkg//", "", "pkg/a.txt", "hi")));
        assertEquals(ErrorCode.PKG_INTEGRITY, e.code());
    }

    @Test
    void rejectsDirectoryEntryWithData() throws Exception {
        CliException e = assertThrows(CliException.class, () -> PackageReader.readZip(zip("pkg/d/", "x", "pkg/a.txt", "hi")));
        assertEquals(ErrorCode.PKG_INTEGRITY, e.code());
    }

    @Test
    void rejectsDotSegmentDirectory() throws Exception {
        CliException e = assertThrows(CliException.class, () -> PackageReader.readZip(zip("pkg/../", "", "pkg/a.txt", "hi")));
        assertEquals(ErrorCode.PKG_INTEGRITY, e.code());
    }
}
