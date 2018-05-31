package code.heartbeat.dto;

import java.io.Serializable;

/**
 *
 */
public class AskParams implements Serializable {
    private static final long serialVersionUID = 1L;
    private String auth;
    private String content;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }
}
