
#[derive(Debug, Clone, Copy, PartialEq)] 
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}

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
            || self.x + self.width< other.x
            || self.y > other.y + other.height
            || self.y + self.height < other.y)
    }
}
