//! In-process `linux_native` isolation: Landlock filesystem rules plus a
//! seccomp socket gate, applied in the child's `pre_exec` hook.

use std::fs;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::process::Command;

use crate::config::SandboxSettings;

use super::{SandboxError, apply_rlimit_values};

/// Lowest Landlock ABI that mediates `truncate(2)` (kernel 6.2). Signal
/// scoping (`LANDLOCK_SCOPE_SIGNAL`) needs ABI 6 (kernel 6.12); below that
/// doctor reports `signal_scoping=none` rather than a half-enforced filter.
pub(super) const MIN_ABI: u32 = 3;

use libc::{SYS_landlock_add_rule, SYS_landlock_create_ruleset, SYS_landlock_restrict_self};

const LANDLOCK_CREATE_RULESET_VERSION: u32 = 1;
const LANDLOCK_RULE_PATH_BENEATH: u32 = 1;

const ACCESS_FS_EXECUTE: u64 = 1 << 0;
const ACCESS_FS_WRITE_FILE: u64 = 1 << 1;
const ACCESS_FS_READ_FILE: u64 = 1 << 2;
const ACCESS_FS_READ_DIR: u64 = 1 << 3;
const ACCESS_FS_REMOVE_DIR: u64 = 1 << 4;
const ACCESS_FS_REMOVE_FILE: u64 = 1 << 5;
const ACCESS_FS_MAKE_CHAR: u64 = 1 << 6;
const ACCESS_FS_MAKE_DIR: u64 = 1 << 7;
const ACCESS_FS_MAKE_REG: u64 = 1 << 8;
const ACCESS_FS_MAKE_SOCK: u64 = 1 << 9;
const ACCESS_FS_MAKE_FIFO: u64 = 1 << 10;
const ACCESS_FS_MAKE_BLOCK: u64 = 1 << 11;
const ACCESS_FS_MAKE_SYM: u64 = 1 << 12;
const ACCESS_FS_REFER: u64 = 1 << 13;
const ACCESS_FS_TRUNCATE: u64 = 1 << 14;
const ACCESS_FS_IOCTL_DEV: u64 = 1 << 15;

const LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET: u64 = 1 << 0;
const LANDLOCK_SCOPE_SIGNAL: u64 = 1 << 1;

const SYSTEM_READ_DIRS: &[&str] = &[
    "/usr",
    "/bin",
    "/sbin",
    "/lib",
    "/lib64",
    "/lib32",
    "/etc/alternatives",
    "/etc/ssl/certs",
];

const SYSTEM_READ_FILES: &[&str] = &[
    "/etc/ld.so.cache",
    "/etc/passwd",
    "/etc/group",
    "/etc/nsswitch.conf",
    "/etc/localtime",
];

const WRITABLE_DEVS: &[&str] = &["/dev/null", "/dev/zero", "/dev/urandom", "/dev/random"];

#[repr(C)]
struct RulesetAttr {
    handled_access_fs: u64,
    handled_access_net: u64,
    scoped: u64,
}

#[repr(C)]
struct PathBeneathAttr {
    allowed_access: u64,
    parent_fd: i32,
}

#[derive(Debug)]
pub(super) struct LinuxPrepared {
    ruleset: OwnedFd,
    pub(super) scratch: PathBuf,
    pub(super) abi: u32,
}

impl Drop for LinuxPrepared {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.scratch);
    }
}

pub(super) fn probe_abi() -> Result<u32, SandboxError> {
    // SAFETY: version probe; null attr and zero size are the documented ABI
    // query. A non-negative return is the kernel's Landlock ABI number.
    let abi = unsafe {
        libc::syscall(
            SYS_landlock_create_ruleset,
            std::ptr::null::<RulesetAttr>(),
            0usize,
            LANDLOCK_CREATE_RULESET_VERSION,
        )
    };
    if abi < 0 {
        return Err(SandboxError::Unavailable(format!(
            "Landlock probe failed: {}",
            io::Error::last_os_error()
        )));
    }
    Ok(abi as u32)
}

pub(super) fn require_abi(abi: u32) -> Result<u32, SandboxError> {
    if abi < MIN_ABI {
        return Err(SandboxError::Unavailable(format!(
            "Landlock ABI {abi} is below the required floor of {MIN_ABI} (kernel 6.2)"
        )));
    }
    Ok(abi)
}

/// Prove the kernel will accept a ruleset and a seccomp filter in a child,
/// not merely report an ABI number.
pub(super) fn probe_enforcement() -> Result<u32, SandboxError> {
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let abi = require_abi(probe_abi()?)?;
    let ruleset = create_ruleset(abi)?;
    let read_dir = ACCESS_FS_EXECUTE | ACCESS_FS_READ_FILE | ACCESS_FS_READ_DIR;
    for path in SYSTEM_READ_DIRS {
        add_existing(&ruleset, Path::new(path), read_dir, abi)?;
    }
    add_existing(
        &ruleset,
        Path::new("/dev/null"),
        ACCESS_FS_READ_FILE | ACCESS_FS_WRITE_FILE,
        abi,
    )?;
    let ruleset_fd = ruleset.as_raw_fd();
    let mut command = Command::new("true");
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::syscall(SYS_landlock_restrict_self, ruleset_fd, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            let (filter, filter_len) = socket_filter(abi, 0);
            let mut prog = libc::sock_fprog {
                len: filter_len,
                filter: filter.as_ptr() as *mut libc::sock_filter,
            };
            if libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::SECCOMP_MODE_FILTER,
                std::ptr::addr_of_mut!(prog),
            ) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    match command.status() {
        Ok(status) if status.success() => Ok(abi),
        Ok(status) => Err(SandboxError::Unavailable(format!(
            "linux_native enforcement probe exited {status}"
        ))),
        Err(error) => Err(SandboxError::Unavailable(format!(
            "linux_native enforcement probe: {error}"
        ))),
    }
}

pub(super) fn prepare(
    settings: &SandboxSettings,
    workspace: &Path,
) -> Result<Arc<LinuxPrepared>, SandboxError> {
    let abi = require_abi(probe_abi()?)?;
    let workspace = fs::canonicalize(workspace).map_err(|error| {
        SandboxError::Invalid(format!(
            "linux_native workspace {}: {error}",
            workspace.display()
        ))
    })?;
    let scratch = create_scratch()?;
    let ruleset = create_ruleset(abi)?;
    add_path_rules(
        &ruleset,
        abi,
        &workspace,
        &scratch,
        &settings.read_only_paths,
    )?;
    Ok(Arc::new(LinuxPrepared {
        ruleset,
        scratch,
        abi,
    }))
}

pub(super) fn apply(
    command: &mut Command,
    settings: &SandboxSettings,
    prepared: Arc<LinuxPrepared>,
) -> Result<(), SandboxError> {
    command.env("TMPDIR", &prepared.scratch);
    command.env("TMP", &prepared.scratch);
    command.env("TEMP", &prepared.scratch);

    let settings = settings.clone();
    let abi = prepared.abi;
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            apply_rlimit_values(&settings)?;
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::syscall(SYS_landlock_restrict_self, prepared.ruleset.as_raw_fd(), 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            // Built here so x32 and identity-change syscalls are denied with
            // a stack-only filter. No allocation.
            let self_pid = libc::getpid() as u32;
            let (filter, filter_len) = socket_filter(abi, self_pid);
            let mut prog = libc::sock_fprog {
                len: filter_len,
                filter: filter.as_ptr() as *mut libc::sock_filter,
            };
            if libc::prctl(
                libc::PR_SET_SECCOMP,
                libc::SECCOMP_MODE_FILTER,
                std::ptr::addr_of_mut!(prog),
            ) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

fn create_scratch() -> Result<PathBuf, SandboxError> {
    let path = std::env::temp_dir().join(format!("shikigami-scratch-{}", uuid::Uuid::new_v4()));
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        SandboxError::Invalid("scratch directory name contains an interior NUL".into())
    })?;
    // SAFETY: `c_path` is a valid C string; 0700 keeps TMPDIR private on a
    // shared host temp directory regardless of umask.
    let rc = unsafe { libc::mkdir(c_path.as_ptr(), 0o700) };
    if rc != 0 {
        return Err(SandboxError::Invalid(format!(
            "scratch directory: {}",
            io::Error::last_os_error()
        )));
    }
    fs::canonicalize(&path)
        .map_err(|error| SandboxError::Invalid(format!("scratch directory: {error}")))
}

fn create_ruleset(abi: u32) -> Result<OwnedFd, SandboxError> {
    let mut attr = RulesetAttr {
        handled_access_fs: handled_fs(abi),
        handled_access_net: 0,
        scoped: if abi >= 6 {
            LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET | LANDLOCK_SCOPE_SIGNAL
        } else {
            0
        },
    };
    let size = ruleset_attr_size(abi);
    // SAFETY: `attr` is a kernel-documented ruleset attribute; `size` matches
    // the fields that ABI understands so extra zeros are not required.
    let fd = unsafe {
        libc::syscall(
            SYS_landlock_create_ruleset,
            std::ptr::addr_of_mut!(attr),
            size,
            0u32,
        )
    };
    if fd < 0 {
        return Err(SandboxError::Unavailable(format!(
            "landlock_create_ruleset: {}",
            io::Error::last_os_error()
        )));
    }
    // SAFETY: the syscall returned a newly owned file descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
}

fn add_path_rules(
    ruleset: &OwnedFd,
    abi: u32,
    workspace: &Path,
    scratch: &Path,
    extra_read_only: &[String],
) -> Result<(), SandboxError> {
    let read_dir = ACCESS_FS_EXECUTE | ACCESS_FS_READ_FILE | ACCESS_FS_READ_DIR;
    let read_file = ACCESS_FS_EXECUTE | ACCESS_FS_READ_FILE;
    let write_dev = ACCESS_FS_READ_FILE | ACCESS_FS_WRITE_FILE | truncate_bit(abi);
    // Workspace/scratch stay ordinary files and directories: device nodes
    // would let a capable child reopen a host disk through an allowed path.
    let write_dir = handled_fs(abi) & !ACCESS_FS_MAKE_CHAR & !ACCESS_FS_MAKE_BLOCK;

    for path in SYSTEM_READ_DIRS {
        add_existing(ruleset, Path::new(path), read_dir, abi)?;
    }
    for path in SYSTEM_READ_FILES {
        add_existing(ruleset, Path::new(path), read_file, abi)?;
    }
    for path in extra_read_only {
        let canonical = fs::canonicalize(path).map_err(|error| {
            SandboxError::Invalid(format!("sandbox.read_only_paths `{path}`: {error}"))
        })?;
        let access = if canonical.is_dir() {
            read_dir
        } else {
            read_file
        };
        add_path(ruleset, &canonical, access, abi)?;
    }
    for path in WRITABLE_DEVS {
        add_existing(ruleset, Path::new(path), write_dev | ioctl_bit(abi), abi)?;
    }
    add_path(ruleset, workspace, write_dir, abi)?;
    add_path(ruleset, scratch, write_dir, abi)?;
    Ok(())
}

fn add_existing(ruleset: &OwnedFd, path: &Path, access: u64, abi: u32) -> Result<(), SandboxError> {
    if !path.exists() {
        return Ok(());
    }
    add_path(ruleset, path, access, abi)
}

fn add_path(ruleset: &OwnedFd, path: &Path, access: u64, abi: u32) -> Result<(), SandboxError> {
    let fd = open_path(path)?;
    let mut attr = PathBeneathAttr {
        allowed_access: access & handled_fs(abi),
        parent_fd: fd.as_raw_fd(),
    };
    // SAFETY: `attr` is a PATH_BENEATH rule whose parent_fd is an open O_PATH
    // descriptor we own for the duration of the syscall.
    let rc = unsafe {
        libc::syscall(
            SYS_landlock_add_rule,
            ruleset.as_raw_fd(),
            LANDLOCK_RULE_PATH_BENEATH,
            std::ptr::addr_of_mut!(attr),
            0u32,
        )
    };
    if rc != 0 {
        return Err(SandboxError::Invalid(format!(
            "landlock_add_rule {}: {}",
            path.display(),
            io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn open_path(path: &Path) -> Result<OwnedFd, SandboxError> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        SandboxError::Invalid(format!(
            "sandbox path {} contains an interior NUL",
            path.display()
        ))
    })?;
    // SAFETY: `c_path` is a valid C string; O_PATH | O_CLOEXEC is the
    // documented way to obtain a Landlock parent_fd.
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(SandboxError::Invalid(format!(
            "open {}: {}",
            path.display(),
            io::Error::last_os_error()
        )));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn handled_fs(abi: u32) -> u64 {
    let mut access = ACCESS_FS_EXECUTE
        | ACCESS_FS_WRITE_FILE
        | ACCESS_FS_READ_FILE
        | ACCESS_FS_READ_DIR
        | ACCESS_FS_REMOVE_DIR
        | ACCESS_FS_REMOVE_FILE
        | ACCESS_FS_MAKE_CHAR
        | ACCESS_FS_MAKE_DIR
        | ACCESS_FS_MAKE_REG
        | ACCESS_FS_MAKE_SOCK
        | ACCESS_FS_MAKE_FIFO
        | ACCESS_FS_MAKE_BLOCK
        | ACCESS_FS_MAKE_SYM;
    if abi >= 2 {
        access |= ACCESS_FS_REFER;
    }
    if abi >= 3 {
        access |= ACCESS_FS_TRUNCATE;
    }
    if abi >= 5 {
        access |= ACCESS_FS_IOCTL_DEV;
    }
    access
}

fn truncate_bit(abi: u32) -> u64 {
    if abi >= 3 { ACCESS_FS_TRUNCATE } else { 0 }
}

fn ioctl_bit(abi: u32) -> u64 {
    if abi >= 5 { ACCESS_FS_IOCTL_DEV } else { 0 }
}

fn ruleset_attr_size(abi: u32) -> usize {
    if abi >= 6 {
        std::mem::size_of::<RulesetAttr>()
    } else if abi >= 4 {
        std::mem::size_of::<u64>() * 2
    } else {
        std::mem::size_of::<u64>()
    }
}

const BPF_LD: u16 = 0x00;
const BPF_W: u16 = 0x00;
const BPF_ABS: u16 = 0x20;
const BPF_ALU: u16 = 0x04;
const BPF_AND: u16 = 0x50;
const BPF_JMP: u16 = 0x05;
const BPF_JEQ: u16 = 0x10;
const BPF_JGE: u16 = 0x30;
const BPF_K: u16 = 0x00;
const BPF_RET: u16 = 0x06;
const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
const SECCOMP_RET_ERRNO_EPERM: u32 = 0x0005_0000 | (libc::EPERM as u32);
/// x32 syscalls share AUDIT_ARCH_X86_64 but set this bit in `nr`.
const X32_SYSCALL_BIT: u32 = 0x4000_0000;

#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xC000_003E;
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = 0xC000_00B7;
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const AUDIT_ARCH: u32 = 0;

fn stmt(code: u16, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

fn jump(code: u16, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
    libc::sock_filter { code, jt, jf, k }
}

fn socket_filter(_abi: u32, _self_pid: u32) -> ([libc::sock_filter; 64], u16) {
    let mut filter = [stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW); 64];
    let mut i = 0;
    let push = |filter: &mut [libc::sock_filter; 64], i: &mut usize, insn: libc::sock_filter| {
        filter[*i] = insn;
        *i += 1;
    };

    // seccomp_data.arch at offset 4; nr at offset 0; args[0] at 16.
    push(&mut filter, &mut i, stmt(BPF_LD | BPF_W | BPF_ABS, 4));
    push(
        &mut filter,
        &mut i,
        jump(BPF_JMP | BPF_JEQ | BPF_K, AUDIT_ARCH, 1, 0),
    );
    push(
        &mut filter,
        &mut i,
        stmt(BPF_RET | BPF_K, SECCOMP_RET_ERRNO_EPERM),
    );
    push(&mut filter, &mut i, stmt(BPF_LD | BPF_W | BPF_ABS, 0));
    // x32 shares AUDIT_ARCH_X86_64; the syscall number has bit 0x40000000 set.
    push(
        &mut filter,
        &mut i,
        jump(BPF_JMP | BPF_JGE | BPF_K, X32_SYSCALL_BIT, 0, 1),
    );
    push(
        &mut filter,
        &mut i,
        stmt(BPF_RET | BPF_K, SECCOMP_RET_ERRNO_EPERM),
    );
    push(
        &mut filter,
        &mut i,
        jump(BPF_JMP | BPF_JEQ | BPF_K, libc::SYS_socket as u32, 0, 1),
    );
    push(
        &mut filter,
        &mut i,
        stmt(BPF_RET | BPF_K, SECCOMP_RET_ERRNO_EPERM),
    );
    // socketpair: only AF_UNIX SOCK_STREAM (pipes, git). Datagram pairs can
    // sendto() a host pathname socket such as /dev/log.
    push(
        &mut filter,
        &mut i,
        jump(BPF_JMP | BPF_JEQ | BPF_K, libc::SYS_socketpair as u32, 0, 7),
    );
    push(&mut filter, &mut i, stmt(BPF_LD | BPF_W | BPF_ABS, 16));
    push(
        &mut filter,
        &mut i,
        jump(BPF_JMP | BPF_JEQ | BPF_K, libc::AF_UNIX as u32, 1, 0),
    );
    push(
        &mut filter,
        &mut i,
        stmt(BPF_RET | BPF_K, SECCOMP_RET_ERRNO_EPERM),
    );
    push(&mut filter, &mut i, stmt(BPF_LD | BPF_W | BPF_ABS, 24));
    push(&mut filter, &mut i, stmt(BPF_ALU | BPF_AND | BPF_K, 0xf));
    push(
        &mut filter,
        &mut i,
        jump(BPF_JMP | BPF_JEQ | BPF_K, libc::SOCK_STREAM as u32, 1, 0),
    );
    push(
        &mut filter,
        &mut i,
        stmt(BPF_RET | BPF_K, SECCOMP_RET_ERRNO_EPERM),
    );
    push(&mut filter, &mut i, stmt(BPF_LD | BPF_W | BPF_ABS, 0));
    // Close alternate socket, signal, and identity-change routes. `setpgid`
    // already ran in this hook; the child must not join the harness group
    // or stand up io_uring (IORING_OP_SOCKET).
    for nr in [
        libc::SYS_pidfd_send_signal as u32,
        libc::SYS_rt_sigqueueinfo as u32,
        libc::SYS_rt_tgsigqueueinfo as u32,
        libc::SYS_io_uring_setup as u32,
        libc::SYS_io_uring_enter as u32,
        libc::SYS_io_uring_register as u32,
        libc::SYS_setpgid as u32,
        libc::SYS_setsid as u32,
        libc::SYS_setns as u32,
        libc::SYS_unshare as u32,
    ] {
        push(
            &mut filter,
            &mut i,
            jump(BPF_JMP | BPF_JEQ | BPF_K, nr, 0, 1),
        );
        push(
            &mut filter,
            &mut i,
            stmt(BPF_RET | BPF_K, SECCOMP_RET_ERRNO_EPERM),
        );
    }

    push(
        &mut filter,
        &mut i,
        stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    );
    (filter, i as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_floor_rejects_truncate_unmediated_kernels() {
        assert!(require_abi(1).is_err());
        assert!(require_abi(2).is_err());
        assert_eq!(require_abi(3).unwrap(), 3);
        assert_eq!(require_abi(6).unwrap(), 6);
    }

    #[test]
    fn probe_reports_a_usable_abi_on_this_host() {
        if let Ok(abi) = probe_abi().and_then(require_abi) {
            assert!(abi >= MIN_ABI, "abi={abi}");
        }
    }

    #[test]
    fn socket_filter_fits_the_fixed_program() {
        let (_filter, len) = socket_filter(3, 1000);
        assert!(len > 8 && len <= 64, "abi3 len={len}");
        let (_filter, len) = socket_filter(6, 1000);
        assert!(len > 4 && len <= 64, "abi6 len={len}");
    }
}
