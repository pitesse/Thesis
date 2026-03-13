package com.polimi.f1.state.groundtruth;

/**
 * state machine phases for pit cycle evaluation. tracks the progress of a pit
 * stop evaluation from initial pit entry through settle period to final rival
 * resolution.
 *
 * PENDING_SETTLE: driver just pitted, waiting for out-lap + settle laps to
 * complete PENDING_RIVAL: driver settled, waiting for rivals to pit or offset
 * timeout
 */
public enum CycleState {

    // waiting for settle period after pit stop (out-lap + 3 laps)
    PENDING_SETTLE,
    // settled, waiting for rival response or offset strategy timeout
    PENDING_RIVAL,
}
