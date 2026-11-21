package aster.truffle;

import aster.truffle.runtime.AllocationBudget;
import aster.truffle.runtime.CallDepthExceededError;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link AllocationBudget#MAX_CALL_DEPTH}：第 10 001 个调用帧进入失败，失败不改变计数，程序外壳帧不计入。 */
class CallDepthLimitTest {

    @Test
    void enterFrameRejectsBeyondLimitWithoutCountingTheFailedFrame() {
        assertEquals(10_000, AllocationBudget.MAX_CALL_DEPTH, "必须与 aster-lang-ts CLI_MAX_CALL_DEPTH 同值");
        // 程序外壳帧不计调用深度：在它之内仍可进入整整 MAX_CALL_DEPTH 个调用帧
        boolean outermost = AllocationBudget.enterFrame();
        assertTrue(outermost);
        for (int i = 0; i < AllocationBudget.MAX_CALL_DEPTH; i++) {
            assertFalse(AllocationBudget.enterCallFrame());
        }
        for (int attempt = 0; attempt < 2; attempt++) {
            CallDepthExceededError e = assertThrows(CallDepthExceededError.class, AllocationBudget::enterCallFrame);
            assertEquals("Maximum call depth (10000) exceeded", e.getMessage());
            // 与真实栈溢出走同一条路（Error，不被 catch (RuntimeException) 拦截）
            assertInstanceOf(StackOverflowError.class, e);
        }
        for (int i = 0; i < AllocationBudget.MAX_CALL_DEPTH; i++) {
            AllocationBudget.exitCallFrame(false);
        }
        assertTrue(AllocationBudget.isInsideGuestFrame());
        AllocationBudget.exitFrame(true);
        assertFalse(AllocationBudget.isInsideGuestFrame());
        // 最外层退出后两个计数都归零：新的一次执行又能进入 MAX_CALL_DEPTH 层
        boolean again = AllocationBudget.enterCallFrame();
        assertTrue(again);
        for (int i = 1; i < AllocationBudget.MAX_CALL_DEPTH; i++) {
            AllocationBudget.enterCallFrame();
        }
        assertThrows(CallDepthExceededError.class, AllocationBudget::enterCallFrame);
        for (int i = 1; i < AllocationBudget.MAX_CALL_DEPTH; i++) {
            AllocationBudget.exitCallFrame(false);
        }
        AllocationBudget.exitCallFrame(true);
        assertFalse(AllocationBudget.isInsideGuestFrame());
    }
}
