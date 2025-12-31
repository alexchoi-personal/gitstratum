use bytes::{BufMut, Bytes, BytesMut};
use std::io::{Read, Write};

use crate::error::{FrontendError, Result};

pub const FLUSH_PKT: &[u8; 4] = b"0000";
pub const DELIM_PKT: &[u8; 4] = b"0001";
pub const RESPONSE_END_PKT: &[u8; 4] = b"0002";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PktLine {
    Data(Bytes),
    Flush,
    Delim,
    ResponseEnd,
}

impl PktLine {
    pub fn data(data: impl Into<Bytes>) -> Self {
        PktLine::Data(data.into())
    }

    pub fn flush() -> Self {
        PktLine::Flush
    }

    pub fn delim() -> Self {
        PktLine::Delim
    }

    pub fn response_end() -> Self {
        PktLine::ResponseEnd
    }

    pub fn is_flush(&self) -> bool {
        matches!(self, PktLine::Flush)
    }

    pub fn is_data(&self) -> bool {
        matches!(self, PktLine::Data(_))
    }

    pub fn as_data(&self) -> Option<&Bytes> {
        match self {
            PktLine::Data(d) => Some(d),
            _ => None,
        }
    }

    pub fn encode(&self) -> Bytes {
        match self {
            PktLine::Flush => Bytes::from_static(FLUSH_PKT),
            PktLine::Delim => Bytes::from_static(DELIM_PKT),
            PktLine::ResponseEnd => Bytes::from_static(RESPONSE_END_PKT),
            PktLine::Data(data) => {
                let len = data.len() + 4;
                let mut buf = BytesMut::with_capacity(len);
                buf.put_slice(format!("{:04x}", len).as_bytes());
                buf.put_slice(data);
                buf.freeze()
            }
        }
    }
}

pub struct PktLineReader<R> {
    reader: R,
}

impl<R: Read> PktLineReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn read_pkt(&mut self) -> Result<Option<PktLine>> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        if &len_buf == FLUSH_PKT {
            return Ok(Some(PktLine::Flush));
        }
        if &len_buf == DELIM_PKT {
            return Ok(Some(PktLine::Delim));
        }
        if &len_buf == RESPONSE_END_PKT {
            return Ok(Some(PktLine::ResponseEnd));
        }

        let len_str = std::str::from_utf8(&len_buf)
            .map_err(|e| FrontendError::InvalidProtocol(format!("invalid pkt-line length: {}", e)))?;
        let len = u16::from_str_radix(len_str, 16)
            .map_err(|e| FrontendError::InvalidProtocol(format!("invalid pkt-line length: {}", e)))?;

        if len < 4 {
            return Err(FrontendError::InvalidProtocol(format!(
                "pkt-line length too small: {}",
                len
            )));
        }

        let data_len = len as usize - 4;
        let mut data = vec![0u8; data_len];
        self.reader.read_exact(&mut data)?;

        Ok(Some(PktLine::Data(Bytes::from(data))))
    }

    pub fn read_all(&mut self) -> Result<Vec<PktLine>> {
        let mut packets = Vec::new();
        while let Some(pkt) = self.read_pkt()? {
            let is_flush = pkt.is_flush();
            packets.push(pkt);
            if is_flush {
                break;
            }
        }
        Ok(packets)
    }
}

pub struct PktLineWriter<W> {
    writer: W,
}

impl<W: Write> PktLineWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn write_pkt(&mut self, pkt: &PktLine) -> Result<()> {
        let encoded = pkt.encode();
        self.writer.write_all(&encoded)?;
        Ok(())
    }

    pub fn write_data(&mut self, data: impl Into<Bytes>) -> Result<()> {
        self.write_pkt(&PktLine::Data(data.into()))
    }

    pub fn write_flush(&mut self) -> Result<()> {
        self.write_pkt(&PktLine::Flush)
    }

    pub fn write_delim(&mut self) -> Result<()> {
        self.write_pkt(&PktLine::Delim)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_pktline_data() {
        let pkt = PktLine::data("hello");
        assert!(pkt.is_data());
        assert!(!pkt.is_flush());
        assert_eq!(pkt.as_data().unwrap().as_ref(), b"hello");
    }

    #[test]
    fn test_pktline_flush() {
        let pkt = PktLine::flush();
        assert!(pkt.is_flush());
        assert!(!pkt.is_data());
        assert!(pkt.as_data().is_none());
    }

    #[test]
    fn test_pktline_encode_flush() {
        let pkt = PktLine::Flush;
        assert_eq!(pkt.encode().as_ref(), b"0000");
    }

    #[test]
    fn test_pktline_encode_delim() {
        let pkt = PktLine::Delim;
        assert_eq!(pkt.encode().as_ref(), b"0001");
    }

    #[test]
    fn test_pktline_encode_response_end() {
        let pkt = PktLine::ResponseEnd;
        assert_eq!(pkt.encode().as_ref(), b"0002");
    }

    #[test]
    fn test_pktline_encode_data() {
        let pkt = PktLine::data("hello");
        let encoded = pkt.encode();
        assert_eq!(&encoded[0..4], b"0009");
        assert_eq!(&encoded[4..], b"hello");
    }

    #[test]
    fn test_pktline_reader_flush() {
        let data = Cursor::new(b"0000");
        let mut reader = PktLineReader::new(data);
        let pkt = reader.read_pkt().unwrap().unwrap();
        assert_eq!(pkt, PktLine::Flush);
    }

    #[test]
    fn test_pktline_reader_delim() {
        let data = Cursor::new(b"0001");
        let mut reader = PktLineReader::new(data);
        let pkt = reader.read_pkt().unwrap().unwrap();
        assert_eq!(pkt, PktLine::Delim);
    }

    #[test]
    fn test_pktline_reader_response_end() {
        let data = Cursor::new(b"0002");
        let mut reader = PktLineReader::new(data);
        let pkt = reader.read_pkt().unwrap().unwrap();
        assert_eq!(pkt, PktLine::ResponseEnd);
    }

    #[test]
    fn test_pktline_reader_data() {
        let data = Cursor::new(b"0009hello");
        let mut reader = PktLineReader::new(data);
        let pkt = reader.read_pkt().unwrap().unwrap();
        assert_eq!(pkt, PktLine::data("hello"));
    }

    #[test]
    fn test_pktline_reader_eof() {
        let data = Cursor::new(b"");
        let mut reader = PktLineReader::new(data);
        let pkt = reader.read_pkt().unwrap();
        assert!(pkt.is_none());
    }

    #[test]
    fn test_pktline_reader_invalid_length() {
        let data = Cursor::new(b"xxxx");
        let mut reader = PktLineReader::new(data);
        assert!(reader.read_pkt().is_err());
    }

    #[test]
    fn test_pktline_reader_length_too_small() {
        let data = Cursor::new(b"0003");
        let mut reader = PktLineReader::new(data);
        assert!(reader.read_pkt().is_err());
    }

    #[test]
    fn test_pktline_reader_read_all() {
        let data = Cursor::new(b"0009hello0009world0000");
        let mut reader = PktLineReader::new(data);
        let packets = reader.read_all().unwrap();
        assert_eq!(packets.len(), 3);
        assert_eq!(packets[0], PktLine::data("hello"));
        assert_eq!(packets[1], PktLine::data("world"));
        assert_eq!(packets[2], PktLine::Flush);
    }

    #[test]
    fn test_pktline_writer() {
        let mut buf = Vec::new();
        {
            let mut writer = PktLineWriter::new(&mut buf);
            writer.write_data("hello").unwrap();
            writer.write_flush().unwrap();
            writer.flush().unwrap();
        }
        assert_eq!(&buf, b"0009hello0000");
    }

    #[test]
    fn test_pktline_writer_delim() {
        let mut buf = Vec::new();
        {
            let mut writer = PktLineWriter::new(&mut buf);
            writer.write_delim().unwrap();
        }
        assert_eq!(&buf, b"0001");
    }

    #[test]
    fn test_pktline_delim_constructor() {
        let pkt = PktLine::delim();
        assert_eq!(pkt, PktLine::Delim);
    }

    #[test]
    fn test_pktline_response_end_constructor() {
        let pkt = PktLine::response_end();
        assert_eq!(pkt, PktLine::ResponseEnd);
    }
}
