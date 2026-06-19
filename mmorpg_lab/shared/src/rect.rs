#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rect {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

impl Rect {
    pub fn contains(&self, pos: &Vec2) -> bool {
        return pos.x >= self.x
            && pos.x < self.x + self.width
            && pos.y >= self.y
            && pos.y < self.y + self.height;
    }

    pub fn min(&self) -> Vec2 {
        return Vec2 {
            x: self.x,
            y: self.y,
        };
    }

    pub fn max(&self) -> Vec2 {
        return Vec2 {
            x: self.x + self.width,
            y: self.y + self.height,
        };
    }

    pub fn center(&self) -> Vec2 {
        return Vec2 {
            x: self.x + self.width / 2.0,
            y: self.y + self.height / 2.0,
        };
    }

    pub fn intersects(&self, other: &Rect) -> bool {
        !(self.x > other.x + other.width
            || self.x + self.width < other.x
            || self.y > other.y + other.height
            || self.y + self.height < other.y)
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&self.x.to_le_bytes());
        buf[4..8].copy_from_slice(&self.y.to_le_bytes());
        buf[8..12].copy_from_slice(&self.width.to_le_bytes());
        buf[12..16].copy_from_slice(&self.height.to_le_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let x = f32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let y = f32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let width = f32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let height = f32::from_le_bytes(bytes[12..16].try_into().unwrap());
        Rect {
            x,
            y,
            width,
            height,
        }
    }
}
