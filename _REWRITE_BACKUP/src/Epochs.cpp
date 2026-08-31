// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Epochs.h"
#include <atomic>
#include <vector>

namespace JLib {
	thread_local size_t thread_id = 0;
}