import {
  asgardeoMiddleware,
  createRouteMatcher,
} from "@asgardeo/nextjs/middleware";
import { NextResponse } from "next/server";

const isPublicRoute = createRouteMatcher([
  "/",
  "/login",
  "/careers",
  "/careers/*",
  "/assessment/*",
  "/interview/*",
  "/offer/*",
  "/api/public/*",
]);

export const proxy = asgardeoMiddleware(
  async (asgardeo, request) => {
    // Signed-in staff skip the public landing page and the login form.
    const { pathname } = request.nextUrl;
    if ((pathname === "/login" || pathname === "/") && asgardeo.isSignedIn()) {
      return NextResponse.redirect(new URL("/dashboard", request.url));
    }

    if (!isPublicRoute(request)) {
      const protectionResult = await asgardeo.protectRoute();

      if (protectionResult) {
        return protectionResult;
      }
    }
  },
  { signInUrl: "/login" },
);

export const config = {
  matcher: [
    "/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)",
    "/(api|trpc)(.*)",
  ],
};
