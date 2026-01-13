#[derive(Clone)]
pub struct GMGlobalConfig {
    pub username: String,
    pub password: String,
    pub login_url: String,
    pub socket_base: String,
}

impl GMGlobalConfig {
    pub fn new() -> Self {
        Self {
            username: "592812".to_string(),
            password: "Abcd6666".to_string(),
            login_url: "https://www.gmglobal.org/ajaxfiles/logincheck.php".to_string(),
            socket_base: "https://thedatamining.org:4003/socket.io/".to_string(),
        }
    }
}
