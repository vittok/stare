"use client";

import { CircleHelp } from "lucide-react";
import Link from "next/link";
import { PortalUpdates } from "./portal-updates";

export function PortalHelp() {
  return (
    <div className="portal-tools">
      <PortalUpdates />
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
