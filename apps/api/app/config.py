from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str | None = None
    app_name: str = "S.T.A.R.E API"
    cors_origins: str = "http://localhost:3000"
    supabase_url: str | None = None
    supabase_publishable_key: str | None = None
    github_actions_token: str | None = None
    github_repository: str = "vittok/stare"
    github_workflow: str = "pipeline_weekdays.yml"
    refresh_allowed_emails: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def refresh_allowed_email_list(self) -> set[str]:
        return {
            email.strip().lower()
            for email in self.refresh_allowed_emails.split(",")
            if email.strip()
        }


@lru_cache
def get_settings() -> Settings:
    return Settings()
