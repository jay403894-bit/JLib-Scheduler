# JLib::Scheduler 5.0.3

**Removes the last code path that named a lambda fiber, rather than merely forbidding one.**

---

## The change

5.0.1 stopped `CreateTask(lambda, lane, TaskType::Fiber)` from compiling by adding an overload that
rejected itself:

```cpp
template<typename F>
auto CreateTask(F&&, Lane, TaskType, ...) {
    static_assert(detail::dependent_false<F>::value, "A lambda task is always Native ...");
    return static_cast<LambdaTask<std::decay_t<F>>*>(nullptr);
}
```

That is a rule about a call. It is still a **declaration in the shipped header** saying the API has
that spelling — and its body names `LambdaTask`. The difference between "you may not do this" and
"this does not exist" is the difference between a policy and a guarantee, and only one of those
survives someone deciding the policy is inconvenient.

It is gone. `detail::dependent_false` went with it; it had no other user.

**Nothing is lost.** Verified by planting the illegal call in a real target and building it:

```
error C2664: 'CreateTask(void (__cdecl *)(void *), void *, Lane, TaskType, CorePref, StackClass)':
  cannot convert argument 1 from 'lambda' to 'void (__cdecl *)(void *)'
```

Still a compile error, and it names the only viable overload — a function pointer — which points at
the fix as directly as the assert did.

## What the API surface is now

Four overloads, and none accepts a callable together with a `TaskType`:

| declaration | first parameter | takes `TaskType`? |
|---|---|---|
| `CreateTask(void(*)(void*), void*, Lane, TaskType, CorePref, StackClass)` | function pointer | yes |
| `CreateInternalTask(void(*)(void*), void*, Lane, CorePref, StackClass)` | function pointer | no |
| `CreateTask(F&&, Lane, CorePref, StackClass)` | callable | **no** |
| `CreateInternalTask(F&&, Lane, CorePref, StackClass)` | callable | **no** |

The lambda form does not take a default it could be argued out of. It pins a constant:

```cpp
constexpr TaskType type = TaskType::Native;
```

A lambda's task type is therefore not a call-site decision at all. It is fixed inside the only
function that can accept a lambda.

## The three gates, restated

A lambda cannot own a fiber row by any route:

1. **No overload accepts it.** Compile error, naming the function-pointer form.
2. **The lambda path pins `Native`.** Not a default — a `constexpr`.
3. **`Thread::AcquireFiber` aborts** if something assigned `type` on the object afterwards
   (5.0.2, enforced in Release, proven by `tests/lambda_fiber_guard_test.cpp`).

## Audit

Every lambda in the tree was checked against the rule, three ways:

- **0** calls pass a callable and a `TaskType`.
- **0** lambda tasks have `type` reassigned, other than the guard test's deliberate violation, which
  runs in a child process and is expected to abort.
- **0** lambda bodies suspend. The two that take a lock are `mutex_block_policy_test`'s control
  (an uncontended Native `Lock()` is permitted — the rule is about *waiting*) and its offender arm,
  which runs with `s_blockViolationHook` installed so it reports instead of aborting.

Every lambda task in the library, tests and benches is Native, and none can become a fiber.

## Compatibility

No API change for any legal call. The only thing that stops compiling is a call that already did
not compile in 5.0.1 and 5.0.2 — the error message changes, the outcome does not. No ABI change.
C++17, no new dependencies.

## Verification

Clean build, both configurations: **53 test binaries, 666 checks, 0 failures, 0 timeouts** in each.
All 7 benches exit 0.
