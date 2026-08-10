package com.bbflight.background_downloader

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class UIDTRunStateTest {
    @Test
    fun completionOwnsJobBeforeLaterStop() {
        val state = UIDTRunState()
        var finished = false

        assertTrue(state.claimFinalization())
        assertTrue(state.finish { finished = true })
        assertEquals(UIDTRunStateValue.FINISHED, state.stop())
        assertTrue(finished)
    }

    @Test
    fun stopOwnsRetryBeforeCompletion() {
        val state = UIDTRunState()

        assertEquals(UIDTRunStateValue.RUNNING, state.stop())
        assertFalse(state.claimFinalization())
        assertFalse(state.finish {})
        assertTrue(state.isStopped())
    }

    @Test
    fun stopDuringFinalizationDoesNotFinishOrReschedule() {
        val state = UIDTRunState()

        assertTrue(state.claimFinalization())
        assertEquals(UIDTRunStateValue.FINALIZING, state.stop())
        assertFalse(state.finish {})
        assertTrue(state.isStopped())
        assertFalse(shouldRescheduleUIDT(UIDTRunStateValue.FINALIZING, false))
    }

    @Test
    fun onlySystemStopWhileRunningReschedules() {
        assertTrue(shouldRescheduleUIDT(UIDTRunStateValue.RUNNING, false))
        assertFalse(shouldRescheduleUIDT(UIDTRunStateValue.RUNNING, true))
        assertFalse(shouldRescheduleUIDT(UIDTRunStateValue.FINISHED, false))
    }
}
