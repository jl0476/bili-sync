use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum BiliError {
    #[error("response missing 'code' field, full response: {0}")]
    InvalidResponse(String),
    #[error("API returned error code {code}, full response: {response}")]
    ErrorResponse {
        code: i64,
        message: Option<String>,
        response: String,
    },
    #[error("risk control triggered by server, full response: {0}")]
    RiskControlOccurred(String),
    #[error("invalid HTTP response code {0}, reason: {1}")]
    InvalidStatusCode(u16, &'static str),
    #[error("no video streams available (may indicate risk control)")]
    VideoStreamsEmpty,
}

impl BiliError {
    pub fn is_risk_control_related(&self) -> bool {
        matches!(
            self,
            BiliError::RiskControlOccurred(_) | BiliError::VideoStreamsEmpty | BiliError::InvalidStatusCode(_, _)
        )
    }

    pub fn is_common_error(&self) -> bool {
        if let BiliError::ErrorResponse { code, message, .. } = self {
            for pair in [(-503, "服务暂不可用"), (-504, "服务调用超时")] {
                if *code == pair.0 && message.as_ref().is_some_and(|m| m == pair.1) {
                    return true;
                }
            }
        }
        false
    }

    /// 判断错误是否表示 UP 主账号不可用（封禁/注销/不存在/冻结）。
    /// 仅匹配 `ErrorResponse` 的 message 关键词，保守集合，可按上线后实测扩充。
    /// 不能用 code 判定：`-404` 既表示用户不存在也表示视频不存在（workflow.rs 中已用于视频）。
    pub fn is_upper_unavailable(&self) -> bool {
        if let BiliError::ErrorResponse { message, .. } = self
            && let Some(msg) = message
        {
            const KEYWORDS: &[&str] = &[
                "该用户不存在",
                "用户不存在",
                "账号已封禁",
                "账号被封禁",
                "已注销",
                "已被冻结",
                "已被封禁",
                "空间已封禁",
            ];
            return KEYWORDS.iter().any(|kw| msg.contains(kw));
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_upper_unavailable() {
        let make = |message: Option<&str>| BiliError::ErrorResponse {
            code: -404,
            message: message.map(String::from),
            response: String::new(),
        };
        // 命中关键词
        assert!(make(Some("该用户不存在")).is_upper_unavailable());
        assert!(make(Some("该账号已封禁")).is_upper_unavailable());
        assert!(make(Some("该账号已被冻结")).is_upper_unavailable());
        assert!(make(Some("空间已封禁")).is_upper_unavailable());
        // 不命中
        assert!(!make(Some("视频不存在")).is_upper_unavailable());
        assert!(!make(None).is_upper_unavailable());
        // 非 ErrorResponse 变体不应被判为 UP 不可用
        assert!(!BiliError::VideoStreamsEmpty.is_upper_unavailable());
        assert!(!BiliError::RiskControlOccurred("x".to_string()).is_upper_unavailable());
    }
}
