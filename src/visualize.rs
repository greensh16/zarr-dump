use anyhow::{Context, Result, anyhow, bail};
use minifb::{Key, KeyRepeat, Window, WindowOptions};

#[derive(Debug, Clone, Copy)]
pub struct ImageView {
    pub width: usize,
    pub height: usize,
    pub stride_y: usize,
    pub stride_x: usize,
}

#[derive(Debug, Clone)]
pub struct SliceDimension {
    pub name: String,
    /// Current index (inclusive)
    pub index: u64,
    /// Maximum index (inclusive)
    pub max: u64,
}

pub fn show_viridis_image(title: &str, data: &[f64], view: ImageView) -> Result<()> {
    let mut window = Window::new(title, view.width, view.height, WindowOptions::default())
        .with_context(
            || "Failed to create window (is an X server available, and is $DISPLAY set?)",
        )?;
    window.set_target_fps(60);

    let mut buffer = vec![0u32; view.width * view.height];
    render_viridis_into_buffer(data, view, &mut buffer)?;

    while window.is_open() {
        if window.is_key_down(Key::Escape) || window.is_key_down(Key::Q) {
            break;
        }

        window
            .update_with_buffer(&buffer, view.width, view.height)
            .context("Failed to update window buffer")?;
    }

    Ok(())
}

pub fn show_viridis_image_with_navigation<F>(
    title_base: &str,
    mut data: Vec<f64>,
    view: ImageView,
    mut dims: Vec<SliceDimension>,
    mut fetch: F,
) -> Result<()>
where
    F: FnMut(&[SliceDimension]) -> Result<Vec<f64>>,
{
    if dims.is_empty() {
        return show_viridis_image(title_base, &data, view);
    }

    let mut active_dim = 0usize;

    let mut window = Window::new(
        &format_title(title_base, &dims, active_dim),
        view.width,
        view.height,
        WindowOptions::default(),
    )
    .with_context(|| "Failed to create window (is an X server available, and is $DISPLAY set?)")?;
    window.set_target_fps(60);

    let mut buffer = vec![0u32; view.width * view.height];
    render_viridis_into_buffer(&data, view, &mut buffer)?;

    while window.is_open() {
        if window.is_key_down(Key::Escape) || window.is_key_down(Key::Q) {
            break;
        }

        let shift = window.is_key_down(Key::LeftShift) || window.is_key_down(Key::RightShift);
        let small_step: u64 = if shift { 10 } else { 1 };
        let big_step: u64 = if shift { 100 } else { 10 };

        let mut changed = false;
        let mut title_changed = false;

        if window.is_key_pressed(Key::Tab, KeyRepeat::No) {
            active_dim = (active_dim + 1) % dims.len();
            title_changed = true;
        }

        if window.is_key_pressed(Key::Left, KeyRepeat::Yes)
            || window.is_key_pressed(Key::Down, KeyRepeat::Yes)
        {
            changed |= dec_index(&mut dims[active_dim], small_step);
        }
        if window.is_key_pressed(Key::Right, KeyRepeat::Yes)
            || window.is_key_pressed(Key::Up, KeyRepeat::Yes)
        {
            changed |= inc_index(&mut dims[active_dim], small_step);
        }

        if window.is_key_pressed(Key::PageDown, KeyRepeat::Yes) {
            changed |= dec_index(&mut dims[active_dim], big_step);
        }
        if window.is_key_pressed(Key::PageUp, KeyRepeat::Yes) {
            changed |= inc_index(&mut dims[active_dim], big_step);
        }

        if window.is_key_pressed(Key::Home, KeyRepeat::No) {
            changed |= set_index(&mut dims[active_dim], 0);
        }
        if window.is_key_pressed(Key::End, KeyRepeat::No) {
            let max = dims[active_dim].max;
            changed |= set_index(&mut dims[active_dim], max);
        }

        if changed {
            data = fetch(&dims)?;
            render_viridis_into_buffer(&data, view, &mut buffer)?;
            title_changed = true;
        }

        if title_changed {
            window.set_title(&format_title(title_base, &dims, active_dim));
        }

        window
            .update_with_buffer(&buffer, view.width, view.height)
            .context("Failed to update window buffer")?;
    }

    Ok(())
}

fn inc_index(dim: &mut SliceDimension, step: u64) -> bool {
    let next = dim.index.saturating_add(step).min(dim.max);
    if next != dim.index {
        dim.index = next;
        true
    } else {
        false
    }
}

fn dec_index(dim: &mut SliceDimension, step: u64) -> bool {
    let next = dim.index.saturating_sub(step);
    if next != dim.index {
        dim.index = next;
        true
    } else {
        false
    }
}

fn set_index(dim: &mut SliceDimension, idx: u64) -> bool {
    let idx = idx.min(dim.max);
    if idx != dim.index {
        dim.index = idx;
        true
    } else {
        false
    }
}

fn format_title(title_base: &str, dims: &[SliceDimension], active_dim: usize) -> String {
    let indices = dims
        .iter()
        .map(|d| format!("{}={}", d.name, d.index))
        .collect::<Vec<_>>()
        .join(", ");

    let active = dims
        .get(active_dim)
        .map(|d| format!("active: {}", d.name))
        .unwrap_or_default();

    format!("{} [{}] ({})", title_base, indices, active)
}

fn render_viridis_into_buffer(data: &[f64], view: ImageView, buffer: &mut [u32]) -> Result<()> {
    if view.width == 0 || view.height == 0 {
        bail!(
            "Cannot plot an empty image ({}x{}).",
            view.width,
            view.height
        );
    }
    if buffer.len() != view.width * view.height {
        bail!(
            "Internal error: pixel buffer has wrong size ({}), expected {}.",
            buffer.len(),
            view.width * view.height
        );
    }

    let max_index = ((view.height - 1)
        .checked_mul(view.stride_y)
        .and_then(|v| v.checked_add((view.width - 1).checked_mul(view.stride_x)?)))
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
    for y in 0..view.height {
        for x in 0..view.width {
            let v = data[y * view.stride_y + x * view.stride_x];
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

    for y in 0..view.height {
        for x in 0..view.width {
            let v = data[y * view.stride_y + x * view.stride_x];
            let pixel = if v.is_finite() {
                let t = ((v - vmin) / denom).clamp(0.0, 1.0);
                let c = colorous::VIRIDIS.eval_continuous(t);
                rgb_u32(c.r, c.g, c.b)
            } else {
                // Non-finite values -> black
                0
            };
            buffer[y * view.width + x] = pixel;
        }
    }

    Ok(())
}

fn rgb_u32(r: u8, g: u8, b: u8) -> u32 {
    (u32::from(r) << 16) | (u32::from(g) << 8) | u32::from(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inc_dec_set_index_clamps() {
        let mut d = SliceDimension {
            name: "time".to_string(),
            index: 0,
            max: 5,
        };

        assert!(inc_index(&mut d, 1));
        assert_eq!(d.index, 1);

        assert!(dec_index(&mut d, 10));
        assert_eq!(d.index, 0);

        assert!(set_index(&mut d, 999));
        assert_eq!(d.index, 5);

        // No-op changes return false
        assert!(!inc_index(&mut d, 1));
        assert!(!set_index(&mut d, 5));
    }

    #[test]
    fn test_format_title_includes_indices_and_active() {
        let dims = vec![
            SliceDimension {
                name: "time".to_string(),
                index: 0,
                max: 10,
            },
            SliceDimension {
                name: "level".to_string(),
                index: 2,
                max: 49,
            },
        ];

        let title = format_title("temp: lat,lon", &dims, 1);
        assert!(title.contains("temp: lat,lon"));
        assert!(title.contains("time=0"));
        assert!(title.contains("level=2"));
        assert!(title.contains("active: level"));
    }
}
