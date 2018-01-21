
pub fn buf2string(buf: &[u8]) -> String {
    let mut ret = String::new();

    for &b in buf {
        ret.push_str(format!("{:02X} ", b).as_str());
    }

    return ret;
}
