use anyhow::{Context, Result, anyhow, bail};
use minifb::{Key, Window, WindowOptions};

pub fn show_viridis_image(
    title: &str,
    data: &[f64],
    width: usize,
    height: usize,
    stride_y: usize,
    stride_x: usize,
) -> Result<()> {
    if width == 0 || height == 0 {
        bail!("Cannot plot an empty image ({}x{}).", width, height);
    }

    let max_index = ((height - 1)
        .checked_mul(stride_y)
        .and_then(|v| v.checked_add((width - 1).checked_mul(stride_x)?)))
    .ok_or_else(|| anyhow!("Internal error: overflow computing maximum data index."))?;

    if max_index >= data.len() {
        bail!(
            "Internal error: data buffer is too small for requested view (need index {}, have length {}).",
            max_index,
            data.len()
        );
    }

    // Compute min/max over the view (ignore non-finite).
    let mut vmin = f64::INFINITY;
    let mut vmax = f64::NEG_INFINITY;
    for y in 0..height {
        for x in 0..width {
            let v = data[y * stride_y + x * stride_x];
            if v.is_finite() {
                vmin = vmin.min(v);
                vmax = vmax.max(v);
            }
        }
    }

    if !vmin.is_finite() || !vmax.is_finite() {
        bail!("Slice contains no finite values.");
    }

    let denom = if (vmax - vmin).abs() > 0.0 {
        vmax - vmin
    } else {
        1.0
    };

    let mut buffer = vec![0u32; width * height];
    for y in 0..height {
        for x in 0..width {
            let v = data[y * stride_y + x * stride_x];
            let pixel = if v.is_finite() {
                let t = ((v - vmin) / denom).clamp(0.0, 1.0);
                let c = colorous::VIRIDIS.eval_continuous(t);
                rgb_u32(c.r, c.g, c.b)
            } else {
                // Non-finite values -> black
                0
            };
            buffer[y * width + x] = pixel;
        }
    }

    let mut window = Window::new(title, width, height, WindowOptions::default()).with_context(
        || "Failed to create window (is an X server available, and is $DISPLAY set?)",
    )?;
    window.set_target_fps(60);

    while window.is_open() {
        if window.is_key_down(Key::Escape) || window.is_key_down(Key::Q) {
            break;
        }

        window
            .update_with_buffer(&buffer, width, height)
            .context("Failed to update window buffer")?;
    }

    Ok(())
}

fn rgb_u32(r: u8, g: u8, b: u8) -> u32 {
    (u32::from(r) << 16) | (u32::from(g) << 8) | u32::from(b)
}
