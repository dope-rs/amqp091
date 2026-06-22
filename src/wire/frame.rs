use dope::manifold::connector;
use o3::buffer::Shared;

use crate::wire::Sink;

pub struct FrameType;

impl FrameType {
    pub const METHOD: u8 = 1;
    pub const HEADER: u8 = 2;
    pub const BODY: u8 = 3;
    pub const HEARTBEAT: u8 = 8;
    pub const END: u8 = 0xCE;
}

pub const PROTOCOL_HEADER: [u8; 8] = [b'A', b'M', b'Q', b'P', 0, 0, 9, 1];

pub const HEADER_LEN: usize = 7;
pub(super) const END_LEN: usize = 1;
pub const OVERHEAD: usize = HEADER_LEN + END_LEN;
pub const MAX_FRAME_HARD_CAP: u32 = 128 * 1024 * 1024;

pub struct Frame {
    pub frame_type: u8,
    pub channel: u16,
    pub payload: Shared,
    pub end_byte: u8,
    pub oversize: bool,
    pub raw_payload_len: u32,
}

pub struct Codec;

impl connector::Codec for Codec {
    type Head = Frame;
    type ParseState = ();

    fn parse(&self, _state: &mut (), buf: &Shared) -> Option<(Frame, usize)> {
        if buf.len() < HEADER_LEN + END_LEN {
            return None;
        }
        let frame_type = buf[0];
        let channel = u16::from_be_bytes([buf[1], buf[2]]);
        let payload_len_raw = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]);
        if payload_len_raw > MAX_FRAME_HARD_CAP {
            return Some((
                Frame {
                    frame_type,
                    channel,
                    payload: buf.slice(0..0),
                    end_byte: 0,
                    oversize: true,
                    raw_payload_len: payload_len_raw,
                },
                HEADER_LEN,
            ));
        }
        let payload_len = payload_len_raw as usize;
        let total = HEADER_LEN + payload_len + END_LEN;
        if buf.len() < total {
            return None;
        }
        let payload = buf.slice(HEADER_LEN..HEADER_LEN + payload_len);
        let end_byte = buf[HEADER_LEN + payload_len];
        Some((
            Frame {
                frame_type,
                channel,
                payload,
                end_byte,
                oversize: false,
                raw_payload_len: payload_len_raw,
            },
            total,
        ))
    }
}

pub struct FrameEncoder;

impl FrameEncoder {
    pub fn write<S: Sink, F: FnOnce(&mut S)>(out: &mut S, frame_type: u8, channel: u16, body: F) {
        let header_pos = out.len();
        for _ in 0..HEADER_LEN {
            out.push(0);
        }
        let payload_start = out.len();
        body(out);
        let payload_len = (out.len() - payload_start) as u32;
        let slice = out.as_mut_slice();
        slice[header_pos] = frame_type;
        slice[header_pos + 1..header_pos + 3].copy_from_slice(&channel.to_be_bytes());
        slice[header_pos + 3..header_pos + 7].copy_from_slice(&payload_len.to_be_bytes());
        out.push(FrameType::END);
    }

    pub fn write_raw_body<S: Sink>(out: &mut S, channel: u16, body: &[u8]) {
        out.push(FrameType::BODY);
        out.extend_from_slice(&channel.to_be_bytes());
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(body);
        out.push(FrameType::END);
    }

    pub fn write_heartbeat<S: Sink>(out: &mut S) {
        out.extend_from_slice(&[FrameType::HEARTBEAT, 0, 0, 0, 0, 0, 0, FrameType::END]);
    }

    pub fn write_protocol_header<S: Sink>(out: &mut S) {
        out.extend_from_slice(&PROTOCOL_HEADER);
    }
}
