    .text
    .globl minirpc_coctx_init
    .type minirpc_coctx_init, @function
minirpc_coctx_init:
#if defined(__x86_64__)
    stmxcsr 112(%rdi)
    fnstcw 116(%rdi)
    ret
#elif defined(__aarch64__)
    stp d8, d9, [x0, #112]
    stp d10, d11, [x0, #128]
    stp d12, d13, [x0, #144]
    stp d14, d15, [x0, #160]
    mrs x1, fpcr
    str x1, [x0, #176]
    mrs x1, fpsr
    str x1, [x0, #184]
    ret
#else
#error "miniRPC coroutine context switch supports x86_64 and aarch64 Linux only"
#endif
    .size minirpc_coctx_init, .-minirpc_coctx_init

    .globl minirpc_coctx_swap
    .type minirpc_coctx_swap, @function
minirpc_coctx_swap:
#if defined(__x86_64__)
    movq %r15, 0(%rdi)
    movq %r14, 8(%rdi)
    movq %r13, 16(%rdi)
    movq %r12, 24(%rdi)
    movq %r9, 32(%rdi)
    movq %r8, 40(%rdi)
    movq %rbp, 48(%rdi)
    movq %rdi, 56(%rdi)
    movq %rsi, 64(%rdi)
    movq (%rsp), %rax
    movq %rax, 72(%rdi)
    movq %rdx, 80(%rdi)
    movq %rcx, 88(%rdi)
    movq %rbx, 96(%rdi)
    leaq 8(%rsp), %rax
    movq %rax, 104(%rdi)
    stmxcsr 112(%rdi)
    fnstcw 116(%rdi)

    movq 0(%rsi), %r15
    movq 8(%rsi), %r14
    movq 16(%rsi), %r13
    movq 24(%rsi), %r12
    movq 32(%rsi), %r9
    movq 40(%rsi), %r8
    movq 48(%rsi), %rbp
    movq 64(%rsi), %rax
    movq 80(%rsi), %rdx
    movq 88(%rsi), %rcx
    movq 96(%rsi), %rbx
    movq 104(%rsi), %rsp
    ldmxcsr 112(%rsi)
    fldcw 116(%rsi)
    pushq 72(%rsi)
    movq 56(%rsi), %rdi
    movq %rax, %rsi
    ret
#elif defined(__aarch64__)
    stp x19, x20, [x0, #0]
    stp x21, x22, [x0, #16]
    stp x23, x24, [x0, #32]
    stp x25, x26, [x0, #48]
    stp x27, x28, [x0, #64]
    str x29, [x0, #80]
    str x30, [x0, #88]
    mov x2, sp
    str x2, [x0, #96]
    str x0, [x0, #104]
    stp d8, d9, [x0, #112]
    stp d10, d11, [x0, #128]
    stp d12, d13, [x0, #144]
    stp d14, d15, [x0, #160]
    mrs x2, fpcr
    str x2, [x0, #176]
    mrs x2, fpsr
    str x2, [x0, #184]

    ldp x19, x20, [x1, #0]
    ldp x21, x22, [x1, #16]
    ldp x23, x24, [x1, #32]
    ldp x25, x26, [x1, #48]
    ldp x27, x28, [x1, #64]
    ldr x29, [x1, #80]
    ldr x30, [x1, #88]
    ldr x2, [x1, #96]
    mov sp, x2
    ldp d8, d9, [x1, #112]
    ldp d10, d11, [x1, #128]
    ldp d12, d13, [x1, #144]
    ldp d14, d15, [x1, #160]
    ldr x2, [x1, #176]
    msr fpcr, x2
    ldr x2, [x1, #184]
    msr fpsr, x2
    ldr x0, [x1, #104]
    ret
#else
#error "miniRPC coroutine context switch supports x86_64 and aarch64 Linux only"
#endif

    .size minirpc_coctx_swap, .-minirpc_coctx_swap
    .section .note.GNU-stack,"",@progbits
