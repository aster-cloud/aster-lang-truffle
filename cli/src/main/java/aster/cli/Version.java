package aster.cli;

import aster.core.embed.UserAliasValidator;
import aster.core.identifier.CaseFold;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** CLI 版本与工具链身份。 */
final class Version {

    /** 构建时写入 {@code aster/cli/version.properties}（= 生态版本）。 */
    static final String CLI_VERSION = load();

    private Version() {}

    private static String load() {
        try (InputStream in = Version.class.getResourceAsStream("version.properties")) {
            if (in == null) {
                return "dev";
            }
            Properties p = new Properties();
            p.load(in);
            return p.getProperty("cliVersion", "dev");
        } catch (IOException e) {
            return "dev";
        }
    }

    /** 与 manifest {@code coreIr.toolchainId} 同形（ADR 0051 §9），build 段为本 CLI。 */
    static String toolchainId() {
        String core = aster.core.canonicalizer.Canonicalizer.class.getPackage().getImplementationVersion();
        return "abi=2.0;core=" + (core == null || core.isBlank() ? "dev" : core)
            + ";validator=" + UserAliasValidator.VERSION
            + ";resolver=1;casefold=" + CaseFold.VERSION
            + ";build=aster-cli-" + CLI_VERSION;
    }
}
