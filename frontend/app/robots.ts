import type { MetadataRoute } from "next";
import { appUrl } from "@/lib/app-url";

export const dynamic = "force-dynamic";

/**
 * The public site (the landing page at `/` and the careers pages) is for search engines. The
 * dashboard, sign-in and the private links sent to candidates are not: they are listed one by one
 * because a crawler cannot be told "everything except". Keep this in step with the top-level
 * folders of app/.
 */
const PRIVATE_PATHS = [
  "/api/",
  "/login",
  "/forgot-password",
  "/reset-password",
  "/dashboard",
  "/jobs",
  "/candidates",
  "/interviews",
  "/offers",
  "/settings",
  "/templates",
  "/assessments",
  "/assessment/",
  "/assignment/",
  "/interview/",
  "/offer/",
];

export default function robots(): MetadataRoute.Robots {
  return {
    rules: { userAgent: "*", allow: "/", disallow: PRIVATE_PATHS },
    sitemap: `${appUrl()}/careers/sitemap.xml`,
  };
}
