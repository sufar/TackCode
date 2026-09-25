import { cn } from "@/components/lib/utils.js";

export function ZCodeAboutLogo({ className }: { className?: string }) {
  // TackCode: pi-rs pincer mark (upstream Z glyph replaced).
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="118"
      height="100"
      fill="none"
      viewBox="-103.0078 -117.394 206.0156 241.394"
      className={cn("shrink-0 text-current", className)}
      aria-hidden="true"
      focusable="false"
    >
      <path
        fill="#F74C00"
        fillRule="evenodd"
        d="M 20 -82 C 13 -91 2 -96 -8 -92 C -32 -62 -62 10 -78 92 C -81 95 -77 99 -70 100 L 70 100 C 77 99 81 95 78 92 C 64 30 46 -36 33 -66 C 29 -56 25 -44 24 -33 C 20 -30 18 -28 17 -26 C 17 -23 19 -20 24 -17 C 30 -5 33 7 31 19 C 17 11 -11 17 -23 25 C -29 7 -9 -39 1 -57 C 4 -67 13 -78 21 -83 Z"
      />
    </svg>
  );
}

export function ZCodeWordmarkLogo({ className }: { className?: string }) {
  // TackCode: text wordmark replacing the ZCODE glyph wordmark.
  return (
    <svg
      width="244"
      height="54"
      viewBox="0 0 244 54"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={cn("shrink-0 text-current", className)}
      aria-hidden="true"
      focusable="false"
    >
      <text
        x="0"
        y="38"
        fill="currentColor"
        fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace"
        fontSize="34"
        fontWeight="600"
        letterSpacing="-1"
      >
        TackCode
      </text>
    </svg>
  );
}
