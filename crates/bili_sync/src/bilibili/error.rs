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
    ///
    /// 此函数是 [`is_upper_permanently_gone`] 与 [`is_upper_banned`] 的并集，保留以兼容旧调用点。
    #[allow(dead_code)]
    pub fn is_upper_unavailable(&self) -> bool {
        self.is_upper_permanently_gone() || self.is_upper_banned()
    }

    /// 判断错误是否表示 UP 主账号已永久不可恢复（注销/不存在）。
    /// 命中后应写入黑名单（Blacklist），不再参与任何巡检。
    pub fn is_upper_permanently_gone(&self) -> bool {
        if let BiliError::ErrorResponse { message, .. } = self
            && let Some(msg) = message
        {
            const KEYWORDS: &[&str] = &["该用户不存在", "用户不存在", "已注销"];
            return KEYWORDS.iter().any(|kw| msg.contains(kw));
        }
        false
    }

    /// 判断错误是否表示 UP 主账号被封禁/冻结（短期或永封无法区分）。
    /// 命中后应进入「封禁观察」状态（Banned），不进黑名单、不进恢复候选，
    /// 由用户人工判断是否转黑名单。
    pub fn is_upper_banned(&self) -> bool {
        if let BiliError::ErrorResponse { message, .. } = self
            && let Some(msg) = message
        {
            const KEYWORDS: &[&str] = &["账号已封禁", "账号被封禁", "已被冻结", "已被封禁", "空间已封禁"];
            return KEYWORDS.iter().any(|kw| msg.contains(kw));
        }
        false
    }

    /// 判断错误是否表示视频不可访问（不存在 / 稿件不可见），应标记为 invalid 跳过后续扫描。
    /// - `-404`：视频不存在（已删除）
    /// - `62002`：稿件不可见（UP 主设为私享 / 审核中 / 被删除等）
    pub fn is_video_inaccessible(&self) -> bool {
        if let BiliError::ErrorResponse { code, .. } = self {
            return *code == -404 || *code == 62002;
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

    #[test]
    fn test_is_upper_permanently_gone_and_banned_split() {
        let make = |message: Option<&str>| BiliError::ErrorResponse {
            code: -404,
            message: message.map(String::from),
            response: String::new(),
        };
        // (message, 期望 permanently_gone, 期望 banned)
        // permanently_gone：注销/不存在
        let cases: &[(&str, bool, bool)] = &[
            ("该用户不存在", true, false),
            ("用户不存在", true, false),
            ("该 UP 已注销", true, false),
            // banned：封禁/冻结
            ("该账号已封禁", false, true),
            ("账号被封禁了", false, true),
            ("该账号已被冻结", false, true),
            ("已被封禁，请联系客服", false, true),
            ("空间已封禁", false, true),
            // 均不命中
            ("视频不存在", false, false),
            ("", false, false),
        ];
        for (msg, exp_gone, exp_banned) in cases {
            let e = make(Some(msg));
            assert_eq!(e.is_upper_permanently_gone(), *exp_gone, "msg={msg:?}");
            assert_eq!(e.is_upper_banned(), *exp_banned, "msg={msg:?}");
            // 并集应等于 is_upper_unavailable
            assert_eq!(e.is_upper_unavailable(), *exp_gone || *exp_banned, "msg={msg:?}");
        }
        // None 与非 ErrorResponse 变体：三个函数都应为 false
        assert!(!make(None).is_upper_permanently_gone());
        assert!(!make(None).is_upper_banned());
        assert!(!BiliError::VideoStreamsEmpty.is_upper_permanently_gone());
        assert!(!BiliError::VideoStreamsEmpty.is_upper_banned());
    }

    #[test]
    fn test_is_video_inaccessible() {
        let make = |code: i64| BiliError::ErrorResponse {
            code,
            message: Some("test".to_string()),
            response: String::new(),
        };
        // 不存在 / 不可见 → 应标记 invalid
        assert!(make(-404).is_video_inaccessible());
        assert!(make(62002).is_video_inaccessible());
        // 其他错误码不应标记 invalid
        assert!(!make(-509).is_video_inaccessible()); // 限频
        assert!(!make(-352).is_video_inaccessible()); // 风控
        assert!(!make(0).is_video_inaccessible());
        // 非 ErrorResponse 变体
        assert!(!BiliError::VideoStreamsEmpty.is_video_inaccessible());
        assert!(!BiliError::RiskControlOccurred("x".to_string()).is_video_inaccessible());
    }
}
