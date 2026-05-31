; =============================================================================
;  APEX Download Manager — Professional Installer Script
;  Inno Setup 6.x  |  LZMA2 Ultra  |  Optional UPX  |  Modular Runtime Layout
; =============================================================================
;
;  Build command (from repo root):
;    iscc installer\APEX_Setup.iss
;
;  Prerequisites:
;    • Inno Setup 6.3+            https://jrsoftware.org/isinfo.php
;    • UPX 4.x (optional)        https://upx.github.io/
;    • Compiled binaries in:     build\Release\
;    • Browser extension ZIPs in: build\extensions\
;
;  Output:
;    build\Output\APEX-v{version}-x64-Setup.exe
; =============================================================================

#define MyAppName      "APEX Download Manager"
#define MyAppAbbr      "ADM"
#define MyAppVersion   "0.1.0"
#define MyAppPublisher "APEX Download Manager Team"
#define MyAppURL       "https://github.com/apex-download-manager"
#define AppExeName     "adm-daemon.exe"
#define AppNativeHost  "adm-native-host.exe"
#define NativeHostID   "com.adm.downloadmanager"
#define ServiceName    "ADMDownloadService"


// Allow CI to override architecture and version via /DAppArch and /DAppVersion
#ifndef AppArch
#define AppArch "x64"
#endif

#ifndef AppVersion
  // AppVersion already defined above; keep existing
#endif

; ── Paths ─────────────────────────────────────────────────────────────────────
#define BinDir         ".\build\Release"
#define ExtDir         "..\build\extensions"
#define AssetsDir      "assets"

[Setup]
; ── Identity ──────────────────────────────────────────────────────────────────
AppId                    = {{A3F1B2C4-9D0E-4F56-8A7B-2E3D5C6F1A9B}}
AppName                  = {#AppName}
AppVersion               = {#AppVersion}
AppVerName               = {#AppName} {#AppVersion}
AppPublisher             = {#AppPublisher}
AppPublisherURL          = {#AppURL}
AppSupportURL            = {#AppURL}/issues
AppUpdatesURL            = {#AppURL}/releases
AppCopyright             = Copyright (C) 2024 {#AppPublisher}

; ── Install layout ────────────────────────────────────────────────────────────
DefaultDirName           = {autopf}\{#MyAppName}
DefaultGroupName         = {#MyAppName}
AllowNoIcons             = yes
DisableProgramGroupPage  = yes
ArchitecturesAllowed     = x64compatible
ArchitecturesInstallIn64BitMode = x64compatible

; ── Output ────────────────────────────────────────────────────────────────────
OutputDir                = ..\build\Output
OutputBaseFilename       = ADM-v{#AppVersion}-{#AppArch}-Setup
SetupIconFile            = {#AssetsDir}\icon.ico
UninstallDisplayIcon     = {app}\bin\{#AppExeName}

; ── Compression: LZMA2 Ultra ──────────────────────────────────────────────────
Compression              = lzma2/ultra64
SolidCompression         = yes
LZMAUseSeparateProcess   = yes
LZMANumBlockThreads      = 4

; ── Wizard appearance ─────────────────────────────────────────────────────────
WizardStyle              = modern
WizardSizePercent        = 110
// Optional wizard images removed or provide files in installer/assets/
// WizardImageFile and WizardSmallImageFile intentionally omitted when not present

; ── Privileges ────────────────────────────────────────────────────────────────
PrivilegesRequired       = lowest
PrivilegesRequiredOverridesAllowed = commandline dialog

; ── Versioning / upgrade ──────────────────────────────────────────────────────
VersionInfoVersion       = {#AppVersion}
VersionInfoProductVersion= {#AppVersion}
VersionInfoCompany       = {#AppPublisher}
VersionInfoDescription   = {#MyAppName} Installer
MinVersion               = 10.0.17763

; ── Misc ──────────────────────────────────────────────────────────────────────
RestartIfNeededByRun     = no
CloseApplications        = yes
CloseApplicationsFilter  = adm-daemon.exe
RestartApplications      = yes
CreateUninstallRegKey    = yes
Uninstallable            = yes

; =============================================================================
[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "arabic";  MessagesFile: "compiler:Languages\Arabic.isl"

; =============================================================================
[Types]
; Installation type options presented to the user
Name: "full";    Description: "Full Installation (Daemon + Native Host + Desktop UI)";
Name: "daemon";  Description: "Core Daemon Only";
Name: "custom";  Description: "Custom Installation"; Flags: iscustom;

; =============================================================================
[Components]
; Modular runtime layout — each component maps to a logical runtime module
Name: "core";        Description: "Core Download Daemon (required)";   \
                     Types: full daemon custom; Flags: fixed;
Name: "native_host"; Description: "Browser Native Messaging Host";      \
                     Types: full custom;
Name: "desktop_ui";  Description: "Slint Desktop UI (optional)";        \
                     Types: full custom;

; =============================================================================
[Tasks]
; Optional user-configurable tasks
Name: "startmenu";       Description: "Create Start Menu shortcuts";                   GroupDescription: "Shortcuts:"; Components: core
Name: "desktopicon";     Description: "Create Desktop shortcut";                        GroupDescription: "Shortcuts:"; Components: desktop_ui; Flags: unchecked
Name: "autostart";       Description: "Start daemon automatically at Windows login";    GroupDescription: "Startup:";   Components: core;       Flags: unchecked
Name: "install_chrome";  Description: "Register native host for Chrome / Chromium";    GroupDescription: "Browsers:";  Components: native_host
Name: "install_firefox"; Description: "Register native host for Firefox";               GroupDescription: "Browsers:";  Components: native_host; Flags: unchecked
Name: "install_edge";    Description: "Register native host for Microsoft Edge";        GroupDescription: "Browsers:";  Components: native_host; Flags: unchecked

; =============================================================================
[Dirs]
; Create the modular runtime directory tree
Name: "{app}";                        Permissions: users-readexec
Name: "{app}\bin";                    Permissions: users-readexec
Name: "{app}\config";                 Permissions: users-modify
Name: "{app}\logs";                   Permissions: users-modify
Name: "{app}\data";                   Permissions: users-modify
Name: "{app}\native-host\manifests";  Permissions: users-readexec; Components: native_host

; =============================================================================
[Files]
; ── Core Daemon ───────────────────────────────────────────────────────────────
Source: "{#BinDir}\adm-daemon.exe";      DestDir: "{app}\bin"; \
    DestName: "adm-daemon.exe";           Flags: ignoreversion; \
    Components: core

; ── Native Messaging Host ─────────────────────────────────────────────────────
Source: "{#BinDir}\adm-native-host.exe"; DestDir: "{app}\bin"; \
    DestName: "adm-native-host.exe";      Flags: ignoreversion; \
    Components: native_host

; ── Native host manifests ─────────────────────────────────────────────────────
Source: "..\apps\native-host\manifests\com.apex.downloadmanager.chromium.json"; \
    DestDir: "{app}\native-host\manifests"; \
    DestName: "chrome.json";              Flags: ignoreversion; \
    Components: native_host

Source: "..\apps\native-host\manifests\com.apex.downloadmanager.firefox.json"; \
    DestDir: "{app}\native-host\manifests"; \
    DestName: "firefox.json";             Flags: ignoreversion; \
    Components: native_host

; ── Desktop UI ────────────────────────────────────────────────────────────────
Source: "{#BinDir}\adm-ui.exe";          DestDir: "{app}\bin"; \
    DestName: "adm-ui.exe";       Flags: ignoreversion skipifsourcedoesntexist; \
    Components: desktop_ui



; ── Default config template ───────────────────────────────────────────────────
Source: "{#AssetsDir}\apex-config-default.toml"; \
    DestDir: "{app}\config"; \
    DestName: "apex.toml";                 Flags: onlyifdoesntexist;

; ── Changelog / Readme ────────────────────────────────────────────────────────
Source: "..\CHANGES.md";                  DestDir: "{app}";     Flags: ignoreversion
Source: "..\README.md";                   DestDir: "{app}";     Flags: ignoreversion

; =============================================================================
[Icons]
; Start Menu
Name: "{group}\{#MyAppName}";      Filename: "{app}\bin\adm-ui.exe"; \
    Tasks: startmenu; Components: desktop_ui
Name: "{group}\{#MyAppAbbr} Daemon";               Filename: "{app}\bin\adm-daemon.exe"; \
    Parameters: "--foreground";          Tasks: startmenu; Components: core
Name: "{group}\Uninstall {#MyAppName}";     Filename: "{uninstallexe}"; \
    Tasks: startmenu

; Desktop shortcut
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\bin\adm-ui.exe"; \
    Tasks: desktopicon; Components: desktop_ui

; =============================================================================
[Registry]
; ── Native Messaging Host — Chrome / Chromium ─────────────────────────────────
Root: HKCU; Subkey: "Software\Google\Chrome\NativeMessagingHosts\{#NativeHostID}"; \
    ValueType: string; ValueName: ""; \
    ValueData: "{app}\native-host\manifests\chrome.json"; \
    Tasks: install_chrome; Components: native_host; Flags: uninsdeletekey

Root: HKCU; Subkey: "Software\Chromium\NativeMessagingHosts\{#NativeHostID}"; \
    ValueType: string; ValueName: ""; \
    ValueData: "{app}\native-host\manifests\chrome.json"; \
    Tasks: install_chrome; Components: native_host; Flags: uninsdeletekey

; ── Native Messaging Host — Microsoft Edge ────────────────────────────────────
Root: HKCU; Subkey: "Software\Microsoft\Edge\NativeMessagingHosts\{#NativeHostID}"; \
    ValueType: string; ValueName: ""; \
    ValueData: "{app}\native-host\manifests\chrome.json"; \
    Tasks: install_edge; Components: native_host; Flags: uninsdeletekey

; ── Native Messaging Host — Firefox ──────────────────────────────────────────
Root: HKCU; Subkey: "Software\Mozilla\NativeMessagingHosts\{#NativeHostID}"; \
    ValueType: string; ValueName: ""; \
    ValueData: "{app}\native-host\manifests\firefox.json"; \
    Tasks: install_firefox; Components: native_host; Flags: uninsdeletekey

; ── Autostart — daemon ────────────────────────────────────────────────────────
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; \
    ValueType: string; ValueName: "APEXDaemon"; \
    ValueData: """{app}\bin\apex-daemon.exe"""; \
    Tasks: autostart; Components: core; Flags: uninsdeletevalue

; ── Uninstall metadata ────────────────────────────────────────────────────────
Root: HKCU; Subkey: "Software\APEX-Download-Manager"; \
    ValueType: string; ValueName: "InstallDir"; \
    ValueData: "{app}"; Flags: uninsdeletekey

; =============================================================================
[Run]
; ── Fix manifest binary paths (post-install) ─────────────────────────────────
Filename: "{sys}\cmd.exe"; \
    Parameters: "/C powershell -NoProfile -Command ""(Get-Content '{app}\native-host\manifests\chrome.json') -replace '/usr/local/bin/apex-native-host', '{app}\bin\apex-native-host.exe'.Replace('\','/') | Set-Content '{app}\native-host\manifests\chrome.json'"""; \
    StatusMsg: "Configuring Chrome native host manifest..."; \
    Flags: runhidden; Components: native_host

Filename: "{sys}\cmd.exe"; \
    Parameters: "/C powershell -NoProfile -Command ""(Get-Content '{app}\native-host\manifests\firefox.json') -replace '/usr/local/bin/apex-native-host', '{app}\bin\apex-native-host.exe'.Replace('\','/') | Set-Content '{app}\native-host\manifests\firefox.json'"""; \
    StatusMsg: "Configuring Firefox native host manifest..."; \
    Flags: runhidden; Components: native_host

; ── Launch daemon after install ───────────────────────────────────────────────
Filename: "{app}\bin\apex-daemon.exe"; \
    Parameters: "--foreground"; \
    Description: "Launch APEX Daemon now"; \
    StatusMsg: "Starting download daemon..."; \
    Flags: postinstall nowait skipifsilent unchecked; \
    Components: core

; ── Launch Desktop UI ─────────────────────────────────────────────────────────
Filename: "{app}\bin\apex-ui.exe"; \
    Description: "Open APEX Download Manager"; \
    Flags: postinstall nowait skipifsilent unchecked; \
    Components: desktop_ui

; =============================================================================
[UninstallRun]
; Stop the daemon gracefully before uninstall
Filename: "{app}\bin\apex.exe"; \
    Parameters: "stop"; \
    Flags: runhidden skipifdoesntexist

; =============================================================================
[Code]
// ── Helpers ──────────────────────────────────────────────────────────────────

// Returns True if {app}\bin is NOT already in the user PATH.
function PathNotContainsApp: Boolean;
var
  Path: string;
begin
  if RegQueryStringValue(HKCU, 'Environment', 'Path', Path) then
    Result := Pos(ExpandConstant('{app}\bin'), Path) = 0
  else
    Result := True;
end;

// ── Pre-install: warn if old daemon is running ───────────────────────────────
function InitializeSetup: Boolean;
begin
  Result := True;
  if CheckForMutexes('ADMDaemonMutex') then begin
    if MsgBox('APEX Download Manager daemon is currently running.'#13#10 +
              'The installer will attempt to stop it. Continue?',
              mbConfirmation, MB_YESNO) = IDNO then
      Result := False;
  end;
end;

// ── Post-install: patch binary path in JSON manifests ────────────────────────
procedure PatchManifestBinaryPath(ManifestFile: string);
var
  Content, BinPath: string;
begin
  if not FileExists(ManifestFile) then Exit;
  if not LoadStringFromFile(ManifestFile, Content) then Exit;

  BinPath := ExpandConstant('{app}\bin\adm-native-host.exe');
  // JSON uses forward slashes — normalise
  StringChangeEx(BinPath, '\', '/', True);

  StringChangeEx(Content, '/usr/local/bin/adm-native-host', BinPath, True);
  SaveStringToFile(ManifestFile, Content, False);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then begin
    if IsComponentSelected('native_host') then begin
      PatchManifestBinaryPath(ExpandConstant('{app}\native-host\manifests\chrome.json'));
      PatchManifestBinaryPath(ExpandConstant('{app}\native-host\manifests\firefox.json'));
    end;
  end;
end;

// ── Upgrade: remove stale PATH entry on uninstall ────────────────────────────
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  Path, AppBin: string;
begin
  if CurUninstallStep = usPostUninstall then begin
    AppBin := ExpandConstant('{app}\bin');
    if RegQueryStringValue(HKCU, 'Environment', 'Path', Path) then begin
      if Pos(AppBin, Path) > 0 then begin
        StringChangeEx(Path, ';' + AppBin, '', True);
        StringChangeEx(Path, AppBin + ';', '', True);
        RegWriteStringValue(HKCU, 'Environment', 'Path', Path);
      end;
    end;
  end;
end;
