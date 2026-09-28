"use client";

import { CircleHelp } from "lucide-react";
import Link from "next/link";
import { PortalAbout } from "./portal-about";
import { PortalUpdates } from "./portal-updates";

type PortalHelpProps = {
  marketDataDate?: string | null;
  portalUpdated?: string | null;
};

export function PortalHelp({ marketDataDate, portalUpdated }: PortalHelpProps) {
  return (
    <div className="portal-tools">
      <PortalUpdates />
      <PortalAbout marketDataDate={marketDataDate} portalUpdated={portalUpdated} />
      <Link
        aria-label="Open portal help"
        className="portal-tool-button help-launcher"
        href="/help"
        title="Help"
      >
        <CircleHelp aria-hidden="true" size={18} strokeWidth={2} />
        <span>Help</span>
      </Link>
    </div>
  );
}
