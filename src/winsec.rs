//! Windows access control: the one model anchor has, said in Windows'
//! vocabulary (docs/09-windows.md §2, §7 Q1).
//!
//! On Unix the boundary is a file mode: the socket is `0600` and the data
//! directory `0700`, so the OS decides who may connect and anchor never
//! authenticates anyone. Windows has no mode bits, so the same sentence is
//! written as a DACL that grants **the creating user's SID and nothing
//! else** -- no Administrators, no SYSTEM, no Everyone. Same position, other
//! kernel: the OS enforces access, anchor still does not authenticate.
//!
//! Three users of it, so it lives here rather than being written three
//! times: the pipe (`server.rs`), the client's check that the pipe it opened
//! is its own user's (`client.rs`), and the data directory (`paths.rs`,
//! `log.rs`).

#![cfg(windows)]

use std::ffi::c_void;
use std::os::windows::io::{AsRawHandle, BorrowedHandle};
use std::path::Path;

use anyhow::{bail, Context, Result};
use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, HANDLE};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT, SE_KERNEL_OBJECT,
};
use windows_sys::Win32::Security::{
    GetSecurityDescriptorDacl, GetTokenInformation, TokenUser, ACL, DACL_SECURITY_INFORMATION,
    OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID,
    SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// The user SID of this process's token, as raw bytes.
///
/// Held as the `TOKEN_USER` buffer it came in, because the SID inside points
/// into that buffer; [`UserSid::as_psid`] is only valid while this lives.
pub struct UserSid {
    buf: Vec<u64>,
}

impl UserSid {
    /// The user this process runs as.
    pub fn current() -> Result<Self> {
        let mut token: HANDLE = std::ptr::null_mut();
        // SAFETY: a pseudo-handle for this process, and an out-pointer we own.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(std::io::Error::last_os_error()).context("opening this process's token");
        }
        let result = (|| {
            let mut len = 0u32;
            // SAFETY: a size query -- null buffer, zero length; it fails with
            // the length it needs written to `len`.
            unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut len) };
            if len == 0 {
                return Err(std::io::Error::last_os_error()).context("sizing TokenUser");
            }
            // u64-aligned, which is at least what TOKEN_USER and a SID need.
            let mut buf = vec![0u64; (len as usize).div_ceil(8)];
            // SAFETY: `buf` holds at least `len` bytes, as asked for above.
            let ok = unsafe {
                GetTokenInformation(token, TokenUser, buf.as_mut_ptr().cast(), len, &mut len)
            };
            if ok == 0 {
                return Err(std::io::Error::last_os_error()).context("reading TokenUser");
            }
            Ok(Self { buf })
        })();
        // SAFETY: closing the token handle opened above, once.
        unsafe { CloseHandle(token) };
        result
    }

    /// A pointer to the SID, valid for as long as `self` is.
    pub fn as_psid(&self) -> PSID {
        // SAFETY: `buf` was filled by GetTokenInformation(TokenUser), so it
        // starts with a TOKEN_USER.
        unsafe { (*self.buf.as_ptr().cast::<TOKEN_USER>()).User.Sid }
    }

    /// The SID in string form, `S-1-5-21-…`. Canonical, so two equal SIDs
    /// always format to the same string -- which is what lets the client
    /// compare owners as strings.
    pub fn to_sid_string(&self) -> Result<String> {
        sid_to_string(self.as_psid())
    }
}

fn sid_to_string(sid: PSID) -> Result<String> {
    let mut wide: *mut u16 = std::ptr::null_mut();
    // SAFETY: `sid` is a valid SID; the out-pointer receives a LocalAlloc'd
    // string that is freed below.
    if unsafe { ConvertSidToStringSidW(sid, &mut wide) } == 0 {
        return Err(std::io::Error::last_os_error()).context("formatting a SID");
    }
    // SAFETY: a NUL-terminated wide string, freed exactly once after copying.
    let text = unsafe {
        let len = (0..).take_while(|&i| *wide.add(i) != 0).count();
        let text = String::from_utf16_lossy(std::slice::from_raw_parts(wide, len));
        LocalFree(wide.cast());
        text
    };
    Ok(text)
}

/// A self-relative security descriptor built from SDDL, freed on drop.
pub struct SecurityDescriptor {
    sd: PSECURITY_DESCRIPTOR,
}

// SAFETY: the descriptor is an immutable heap block after construction; the
// pointer is only ever read.
unsafe impl Send for SecurityDescriptor {}
// SAFETY: as above.
unsafe impl Sync for SecurityDescriptor {}

impl SecurityDescriptor {
    /// Owner `user`, and a protected DACL granting `user` full access and
    /// nobody anything -- for the pipe.
    ///
    /// The owner is written explicitly rather than left to the token's
    /// default, because the default owner of an elevated token is the
    /// Administrators group, and the client's server-identity check reads
    /// exactly this field (docs/04-protocol.md#windows-a-named-pipe).
    /// `P` protects the DACL from inheriting anything.
    pub fn owner_only_pipe(user: &UserSid) -> Result<Self> {
        let sid = user.to_sid_string()?;
        Self::from_sddl(&format!("O:{sid}D:P(A;;GA;;;{sid})"))
    }

    /// A protected DACL granting `user` full access, inherited by every file
    /// and directory created beneath -- for the data directory.
    pub fn owner_only_dir(user: &UserSid) -> Result<Self> {
        let sid = user.to_sid_string()?;
        Self::from_sddl(&format!("D:P(A;OICI;FA;;;{sid})"))
    }

    /// Any descriptor, from SDDL. Public so tests can build the hostile
    /// pipes the server and client must refuse.
    pub fn from_sddl(sddl: &str) -> Result<Self> {
        let wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: a NUL-terminated wide string; `sd` receives a LocalAlloc'd
        // descriptor this type frees on drop.
        let ok = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut sd,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("building a security descriptor from {sddl}"));
        }
        Ok(Self { sd })
    }

    /// This descriptor's owner, as SDDL writes it. For tests: SDDL spells
    /// some SIDs by alias -- the built-in Administrator (RID 500, which is
    /// the user GitHub's Windows runner runs as) reads back as `LA`, not
    /// `S-1-5-21-...-500` -- so an assertion on read-back SDDL has to ask
    /// how *this* user is spelled rather than assume the string form.
    pub fn owner_sddl(&self) -> Result<String> {
        format_sddl(self.sd, OWNER_SECURITY_INFORMATION)
    }

    /// `SECURITY_ATTRIBUTES` pointing at this descriptor, valid while `self`
    /// is. Not inheritable: no child process gets the handle.
    pub fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.sd,
            bInheritHandle: 0,
        }
    }

    fn dacl(&self) -> Result<*mut ACL> {
        let mut present = 0;
        let mut defaulted = 0;
        let mut dacl: *mut ACL = std::ptr::null_mut();
        // SAFETY: `sd` is a valid descriptor; the out-pointers are ours.
        let ok =
            unsafe { GetSecurityDescriptorDacl(self.sd, &mut present, &mut dacl, &mut defaulted) };
        if ok == 0 || present == 0 {
            bail!("the security descriptor has no DACL");
        }
        Ok(dacl)
    }
}

impl Drop for SecurityDescriptor {
    fn drop(&mut self) {
        // SAFETY: allocated by ConvertStringSecurityDescriptorToSecurityDescriptorW.
        unsafe { LocalFree(self.sd) };
    }
}

/// Replaces `path`'s DACL with one granting this user alone, protected from
/// inheriting its parent's.
///
/// Applied whether or not the directory is new, like the Unix `chmod 0700`:
/// a directory left by an older build, or created by hand, is exactly the
/// case worth correcting. Everything created beneath inherits it.
///
/// "Alone" in the DACL's sense. An administrator can still take ownership
/// and read anything, as root can on Unix; what this rules out is the
/// default -- every interactive user and service on the machine able to
/// read every session's transcript.
pub fn restrict_to_current_user(path: &Path) -> Result<()> {
    let user = UserSid::current()?;
    let sd = SecurityDescriptor::owner_only_dir(&user)?;
    let dacl = sd.dacl()?;
    let wide: Vec<u16> = path
        .as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain(Some(0))
        .collect();
    // SAFETY: a NUL-terminated path and a DACL that lives as long as `sd`.
    let rc = unsafe {
        SetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            dacl,
            std::ptr::null(),
        )
    };
    if rc != 0 {
        return Err(std::io::Error::from_raw_os_error(rc as i32))
            .with_context(|| format!("restricting {} to its owner", path.display()));
    }
    Ok(())
}

/// The owner SID of the kernel object behind `handle`, as a string.
///
/// For the client's check of the pipe it opened, and for tests that read
/// back what the server set.
pub fn handle_owner(handle: BorrowedHandle<'_>) -> Result<String> {
    let mut owner: PSID = std::ptr::null_mut();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: a live handle; `owner` points into `sd`, which is freed below
    // once the owner has been formatted.
    let rc = unsafe {
        GetSecurityInfo(
            handle.as_raw_handle(),
            SE_KERNEL_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut sd,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::from_raw_os_error(rc as i32))
            .context("reading the pipe's owner");
    }
    let owner = sid_to_string(owner);
    // SAFETY: allocated by GetSecurityInfo; `owner` is no longer used.
    unsafe { LocalFree(sd.cast::<c_void>()) };
    owner
}

/// The owner and DACL of the object behind `handle`, as SDDL -- what the
/// kernel actually holds, for tests to assert against rather than the
/// string the descriptor was built from.
pub fn handle_sddl(handle: BorrowedHandle<'_>) -> Result<String> {
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    let info = OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
    // SAFETY: a live handle; `sd` is freed below.
    let rc = unsafe {
        GetSecurityInfo(
            handle.as_raw_handle(),
            SE_KERNEL_OBJECT,
            info,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut sd,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::from_raw_os_error(rc as i32))
            .context("reading a handle's DACL");
    }
    descriptor_to_sddl(sd, info)
}

/// The DACL of the file or directory at `path`, as SDDL.
pub fn path_sddl(path: &Path) -> Result<String> {
    use windows_sys::Win32::Security::Authorization::GetNamedSecurityInfoW;
    let wide: Vec<u16> = path
        .as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain(Some(0))
        .collect();
    let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    // SAFETY: a NUL-terminated path; `sd` is freed below.
    let rc = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut sd,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::from_raw_os_error(rc as i32))
            .with_context(|| format!("reading the DACL of {}", path.display()));
    }
    descriptor_to_sddl(sd, DACL_SECURITY_INFORMATION)
}

/// Formats `sd` and frees it.
fn descriptor_to_sddl(sd: PSECURITY_DESCRIPTOR, info: u32) -> Result<String> {
    let result = format_sddl(sd, info);
    // SAFETY: allocated by GetSecurityInfo / GetNamedSecurityInfoW.
    unsafe { LocalFree(sd) };
    result
}

/// Formats `sd` as SDDL, leaving it allocated.
fn format_sddl(sd: PSECURITY_DESCRIPTOR, info: u32) -> Result<String> {
    use windows_sys::Win32::Security::Authorization::ConvertSecurityDescriptorToStringSecurityDescriptorW;
    let mut wide: *mut u16 = std::ptr::null_mut();
    let mut len = 0u32;
    // SAFETY: a valid descriptor; the string is LocalAlloc'd and freed below.
    let ok = unsafe {
        ConvertSecurityDescriptorToStringSecurityDescriptorW(
            sd,
            SDDL_REVISION_1,
            info,
            &mut wide,
            &mut len,
        )
    };
    let result = if ok == 0 {
        Err(std::io::Error::last_os_error()).context("formatting a security descriptor")
    } else {
        // SAFETY: `len` wide chars including the NUL; freed once below.
        let text =
            unsafe { String::from_utf16_lossy(std::slice::from_raw_parts(wide, len as usize)) };
        // SAFETY: allocated by the call above.
        unsafe { LocalFree(wide.cast()) };
        Ok(text.trim_end_matches('\0').to_string())
    };
    result
}
