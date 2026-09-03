"use client";

import Image from "next/image";
import type { PointerEvent, ReactNode } from "react";
import { AuthButton } from "./auth-button";
import { PortalHelp } from "./portal-help";

type LoginExperienceProps = {
  children: ReactNode;
  signedIn: boolean;
};

export function LoginExperience({ children, signedIn }: LoginExperienceProps) {
  if (signedIn) return <><PortalHelp />{children}</>;

  function movePreview(event: PointerEvent<HTMLDivElement>) {
    const bounds = event.currentTarget.getBoundingClientRect();
    const horizontal = ((event.clientX - bounds.left) / bounds.width - 0.5) * -18;
    const vertical = ((event.clientY - bounds.top) / bounds.height - 0.5) * -12;

    event.currentTarget.style.setProperty("--login-shift-x", `${horizontal.toFixed(1)}px`);
    event.currentTarget.style.setProperty("--login-shift-y", `${vertical.toFixed(1)}px`);
  }

  function resetPreview(event: PointerEvent<HTMLDivElement>) {
    event.currentTarget.style.setProperty("--login-shift-x", "0px");
    event.currentTarget.style.setProperty("--login-shift-y", "0px");
  }

  return (
    <div className="login-experience" onPointerLeave={resetPreview} onPointerMove={movePreview}>
      <PortalHelp />
      <div aria-hidden="true" className="login-preview" inert>
        {children}
      </div>

      <div className="login-overlay">
        <section aria-labelledby="login-title" aria-modal="true" className="login-dialog" role="dialog">
          <div className="login-dialog-brand">
            <Image alt="S.T.A.R.E logo" height={72} priority src="/Logo.png" width={72} />
            <div>
              <span>Sector &amp; Stock Trend Analysis Engine</span>
              <strong>S.T.A.R.E</strong>
            </div>
          </div>
          <div className="login-dialog-copy">
            <p className="eyebrow">Personal market portal</p>
            <h1 id="login-title">Sign in to continue</h1>
            <p>Use your Google account to open your dashboard and saved preferences.</p>
          </div>
          <AuthButton className="button login-google-button" label="Continue with Google" signedIn={false} />
          <p className="login-disclaimer">Market research signals are informational and are not personalized financial advice.</p>
        </section>
      </div>
    </div>
  );
}
