fn main() {
    slint_build::compile("ui/app-window.slint").unwrap();

    // Embed the canonical application icon (single source of truth).
    // The icon lives in installer/assets/ — do not duplicate it under src/.
    #[cfg(target_os = "windows")]
    {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("../../installer/assets/icon.ico");
        res.compile().expect("failed to compile Windows resources");
    }
}
