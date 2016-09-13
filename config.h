#pragma once

// Enables probe phase.
#define ENABLE_PROBE 0

// Enables tracking of transaction abort codes.
#define TM_TRACK 0

// Enables retry of failed transactions and conflicts.
#define TM_RETRY 1

// Enables adaptive transaction size.
#define HTM_ADAPTIVE 0

// Runs a pre-pass to see if we want to switch to radix-join.
#define HTM_SWITCH 0

