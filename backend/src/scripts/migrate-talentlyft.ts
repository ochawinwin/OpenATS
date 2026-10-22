/**
 * One-off migration: active (Published) jobs + their candidates from TalentLyft into OpenATS.
 *
 *   TALENTLYFT_API_KEY=... node dist/src/scripts/migrate-talentlyft.js                                   # dry run (rolled back)
 *   TALENTLYFT_API_KEY=... node dist/src/scripts/migrate-talentlyft.js --apply --created-by=me@acme.com  # write for real
 *
 * Flags:
 *   --apply                       write for real (default is a dry run that rolls every transaction back)
 *   --created-by=<email>          OpenATS user that owns the jobs/offers/notes (required with --apply)
 *   --job=<tlJobId>               only this TalentLyft job (repeatable)
 *   --candidates=<id,id,...>      only these TalentLyft candidate ids (for a trial run)
 *   --skip-resumes                do not mirror files to R2
 *   --mock-email-domain=<domain>  domain for candidates without an email (default no-email.invalid)
 *   --report=<path>               also write the JSON report to a file
 *
 * Safe to re-run: a job is identified by its slug (`tl-<id>-...`) and skipped if it exists, every job is
 * migrated in a single transaction, and uploaded files use deterministic R2 keys.
 * A dry run executes the whole flow but rolls each transaction back and never uploads to R2.
 */
import "dotenv/config";
import fs from "fs";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { asc, eq, sql } from "drizzle-orm";
import { db } from "../db";
import {
  candidateActivities,
  candidateChatMessages,
  candidateCustomAnswers,
  candidateRejections,
  candidateStageHistory,
  candidates,
  company,
  departments,
  jobCustomQuestions,
  jobHiringTeam,
  jobPipelineStages,
  jobs,
  offers,
  pipelineStageTemplates,
  users,
} from "../db/schema";

const TL_BASE = "https://api.talentlyft.com/v2";

// ───────────────────────── CLI ─────────────────────────

const argv = process.argv.slice(2);
const flag = (name: string) => argv.includes(`--${name}`);
const values = (name: string) =>
  argv.filter((a) => a.startsWith(`--${name}=`)).map((a) => a.slice(name.length + 3));

const APPLY = flag("apply");
const SKIP_RESUMES = flag("skip-resumes");
const ONLY_JOBS = values("job").map(Number);
const ONLY_CANDIDATES = values("candidates")
  .flatMap((v) => v.split(","))
  .map(Number)
  .filter(Boolean);
const CREATED_BY_EMAIL = values("created-by")[0];
const MOCK_EMAIL_DOMAIN = values("mock-email-domain")[0] ?? "no-email.invalid";
const REPORT_PATH = values("report")[0];

// ───────────────────────── TalentLyft types (only what we read) ─────────────────────────

type TlPaged<T> = { Results: T[]; Count: number; Pages: { Next: string | null } };
type TlFile = { Id?: number; FileName: string; Url: string; ContentType?: string | null };
type TlJob = {
  Id: number;
  Title: string;
  Description: string | null;
  Requirements: string | null;
  Department: { Name: string } | null;
  Location: { CityCountry?: string | null } | null;
  Contract: { Text: string | null } | null;
  Status: string;
};
type TlQuestion = {
  Id: number;
  DisplayName: string;
  Type: string;
  Position: number;
  Required: boolean;
};
type TlForm = {
  FormFields: { Key: string; DisplayName: string; Required: boolean }[];
  Questions: TlQuestion[];
};
type TlApplicationSummary = {
  Id: number;
  Created: string;
  Disqualified: boolean;
  DisqualifiedAt: string | null;
  DisqualificationReason: string | null;
  DisqualificationType: string | null;
  InStageSince: string | null;
  Stage: { Name: string; Type: string };
  Job: { Id: number };
  Source: { SourceFormatted?: string | null } | null;
};
type TlCandidateSummary = {
  Id: number;
  FirstName: string | null;
  LastName: string | null;
  LastModified: string | null;
  Tags: string[] | null;
  Source: { SourceFormatted?: string | null } | null;
  Emails: { Value: string; IsPrimary: boolean }[];
  PhoneNumbers: { Value: string; IsPrimary: boolean }[];
  Applications: TlApplicationSummary[];
};
type TlExperience = {
  Title?: string | null;
  Company?: string | null;
  Description?: string | null;
  From?: string | null;
  To?: string | null;
  Current?: boolean;
};
type TlEducation = {
  Institution?: string | null;
  Degree?: string | null;
  Description?: string | null;
  From?: string | null;
  To?: string | null;
  Current?: boolean;
};
type TlApplicationDetail = {
  CoverLetter: string | null;
  Resume: TlFile | null;
  Attachments: TlFile[] | null;
  Experiences: TlExperience[] | null;
  Educations: TlEducation[] | null;
  Answers:
    | {
        Id: number;
        Type: string;
        Answer?: { Response?: string | null; File?: TlFile | null } | null;
      }[]
    | null;
};
type TlActivity = {
  ActivityTypeEnum: string;
  Created: string;
  Member: { Name: string } | null;
  Stage: { Name: string; Type: string } | null;
  Job: { Id: number } | null;
  Application: { Id: number } | null;
  Note: { Message: string | null } | null;
};

// ───────────────────────── helpers ─────────────────────────

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function tl<T>(path: string): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(`${TL_BASE}${path}`, {
      headers: { Authorization: `Bearer ${process.env.TALENTLYFT_API_KEY}` },
    });
    if (res.ok) return (await res.json()) as T;
    const retryable = res.status === 429 || res.status >= 500;
    if (!retryable || attempt >= 5) {
      throw new Error(`TalentLyft ${res.status} on ${path}: ${(await res.text()).slice(0, 200)}`);
    }
    await sleep(1000 * 2 ** attempt);
  }
}

async function tlAll<T>(path: string): Promise<T[]> {
  const out: T[] = [];
  const sep = path.includes("?") ? "&" : "?";
  for (let page = 1; ; page++) {
    const body = await tl<TlPaged<T>>(`${path}${sep}perPage=100&page=${page}`);
    out.push(...body.Results);
    if (!body.Pages.Next) return out;
  }
}

async function mapPool<T, R>(items: T[], size: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(size, items.length) }, async () => {
      while (next < items.length) {
        const i = next++;
        out[i] = await fn(items[i] as T);
      }
    }),
  );
  return out;
}

function slugify(title: string): string {
  const s = title
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-");
  return s.slice(0, 120);
}

/** Descriptions are rendered with dangerouslySetInnerHTML, so strip anything executable. */
function sanitizeHtml(html: string): string {
  return html
    .replace(/<(script|style|iframe|object|embed)\b[\s\S]*?<\/\1>/gi, "")
    .replace(/<(script|style|iframe|object|embed)\b[^>]*\/?>/gi, "")
    .replace(/\son[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, "")
    .replace(/(href|src)\s*=\s*("|')\s*javascript:[^"']*\2/gi, '$1="#"');
}

/** Candidate chat messages are plain text, so flatten TalentLyft's HTML notes. */
function htmlToText(html: string): string {
  return html
    .replace(/<\s*(br|hr)\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|h[1-6]|li|tr)>/gi, "\n")
    .replace(/<li[^>]*>/gi, "- ")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/** Postgres rejects NUL bytes in text columns. */
const pg = (s: string) => s.replace(/\u0000/g, "");

const day = (iso: string | null | undefined) => (iso ? iso.slice(0, 10) : "?");

type EmploymentType = "full_time" | "part_time" | "contract" | "internship" | "freelance";

/**
 * 1) a keyword in the title's brackets, e.g. "Back-End Developer (Contract)" -> contract
 * 2) TalentLyft's contract field  3) the department ("Internship")  4) full time
 */
function mapEmploymentType(
  title: string,
  contract: string | null | undefined,
  department: string | undefined,
): { value: EmploymentType; source: "title" | "talentlyft" | "department" | "default" } {
  const brackets = [...title.matchAll(/[(\[]([^)\]]*)[)\]]/g)].map((m) => m[1] ?? "").join(" ").toLowerCase();
  if (brackets.includes("intern")) return { value: "internship", source: "title" };
  if (brackets.includes("freelance")) return { value: "freelance", source: "title" };
  if (brackets.includes("contract")) return { value: "contract", source: "title" };
  if (/part[\s-]?time/.test(brackets)) return { value: "part_time", source: "title" };

  const c = (contract ?? "").toLowerCase();
  if (c.includes("intern")) return { value: "internship", source: "talentlyft" };
  if (c.includes("freelance") || c.includes("consult")) return { value: "freelance", source: "talentlyft" };
  if (c.includes("part")) return { value: "part_time", source: "talentlyft" };
  if (c.includes("fixed") || c.includes("temporary") || c.includes("contract"))
    return { value: "contract", source: "talentlyft" };
  if (c.includes("permanent") || c.includes("full")) return { value: "full_time", source: "talentlyft" };

  if ((department ?? "").toLowerCase().includes("intern")) return { value: "internship", source: "department" };
  return { value: "full_time", source: "default" };
}

type QuestionType = "short_answer" | "long_answer" | "url";
function mapQuestionType(type: string): QuestionType | null {
  switch (type) {
    case "text":
      return "short_answer";
    case "textarea":
      return "long_answer";
    case "url":
    case "file": // OpenATS has no file question; the uploaded file is stored as a link
      return "url";
    default:
      return null;
  }
}

/** TalentLyft pipeline stage type -> OpenATS default stage template name. */
function mapStageName(type: string): string {
  switch (type) {
    case "Screen":
      return "Screening Qualified";
    case "Interview":
    case "Assessment":
      return "Interviews";
    case "Shortlisted":
      return "Shortlisted";
    case "Offer":
      return "Offer";
    case "Hired":
      return "Hired";
    default: // Sourced, Applied, anything unknown
      return "Screening";
  }
}

// ───────────────────────── R2 ─────────────────────────

let r2: S3Client | null = null;
function r2Client(): S3Client {
  r2 ??= new S3Client({
    region: "us-east-1",
    endpoint: process.env.R2_ENDPOINT!,
    credentials: {
      accessKeyId: process.env.R2_ACCESS_KEY_ID!,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
    },
    forcePathStyle: true,
  });
  return r2;
}

const EXT_BY_MIME: Record<string, string> = {
  "application/pdf": ".pdf",
  "application/msword": ".doc",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
  "application/rtf": ".rtf",
  "text/rtf": ".rtf",
  "text/plain": ".txt",
  "image/png": ".png",
  "image/jpeg": ".jpg",
};

const stats = {
  resumesUploaded: 0,
  answerFilesUploaded: 0,
  attachmentsUploaded: 0,
  fileFailures: [] as string[],
};

/** Download a (short-lived, signed) TalentLyft file and store it in R2 under a deterministic key. */
async function mirrorFile(file: TlFile, keyBase: string): Promise<string | null> {
  const res = await fetch(file.Url);
  if (!res.ok) {
    stats.fileFailures.push(`${keyBase}: download ${res.status}`);
    return null;
  }
  const buf = Buffer.from(await res.arrayBuffer());
  const mime = file.ContentType ?? res.headers.get("content-type") ?? "application/octet-stream";
  const ext = EXT_BY_MIME[mime] ?? (/\.[a-z0-9]{2,5}$/i.exec(file.FileName)?.[0]?.toLowerCase() ?? "");
  const key = `resumes/${keyBase}${ext}`;
  await r2Client().send(
    new PutObjectCommand({
      Bucket: process.env.R2_BUCKET_NAME!,
      Key: key,
      Body: buf,
      ContentType: mime,
      ContentDisposition: mime === "application/pdf" ? "inline" : "attachment",
    }),
  );
  const base = process.env.R2_PUBLIC_URL!.replace(/\/$/, "");
  return `${base}/${key}`;
}

// ───────────────────────── per-job migration ─────────────────────────

type CandidatePlan = {
  tlCandidateId: number;
  tlApplicationId: number;
  firstName: string;
  lastName: string;
  email: string;
  mockEmail: boolean;
  phone: string | null;
  resumeUrl: string | null;
  stageName: string;
  status: "active" | "rejected" | "offered" | "hired" | "withdrawn";
  appliedAt: Date;
  updatedAt: Date;
  stageSince: Date;
  stageHistory: { stageName: string; at: Date }[];
  rejection: { reason: string | null; note: string | null; at: Date } | null;
  coverLetter: string | null;
  answers: { tlQuestionId: number; text: string }[];
  /** recruiter comments from the TalentLyft timeline */
  comments: { at: Date; text: string }[];
  /** one "imported from TalentLyft" note: source, tags, experience, education, extra files */
  importNote: string;
  hadResumeFile: boolean;
};

type JobReport = {
  tlJobId: number;
  title: string;
  status: "migrated" | "skipped_exists" | "failed";
  openatsJobId?: number;
  employmentType?: string;
  employmentTypeSource?: string;
  department?: string;
  departmentCreated?: boolean;
  questions: number;
  questionsFromFileType: number;
  unsupportedQuestions: string[];
  candidatesTotal: number;
  candidatesInserted: number;
  mockEmails: string[];
  duplicatesSkipped: string[];
  skipped: string[];
  withoutResume: number;
  commentsMigrated: number;
  offersCreated: number;
  byStage: Record<string, number>;
  byStatus: Record<string, number>;
  rejectionTypes: Record<string, number>;
  error?: string;
};

class Rollback extends Error {}

type StageTemplate = {
  id: number;
  name: string;
  position: number;
  stageType: "screening" | "interview" | "offer";
};

async function migrateJob(
  job: TlJob,
  ctx: { createdBy: number; companyId: number; templates: StageTemplate[] },
): Promise<JobReport> {
  const report: JobReport = {
    tlJobId: job.Id,
    title: job.Title,
    status: "migrated",
    questions: 0,
    questionsFromFileType: 0,
    unsupportedQuestions: [],
    candidatesTotal: 0,
    candidatesInserted: 0,
    mockEmails: [],
    duplicatesSkipped: [],
    skipped: [],
    withoutResume: 0,
    commentsMigrated: 0,
    offersCreated: 0,
    byStage: {},
    byStatus: {},
    rejectionTypes: {},
  };

  const slug = `tl-${job.Id}-${slugify(job.Title)}`.replace(/-+$/, "");
  const [existing] = await db.select({ id: jobs.id }).from(jobs).where(eq(jobs.slug, slug));
  if (existing) {
    report.status = "skipped_exists";
    report.openatsJobId = existing.id;
    return report;
  }

  // ── Phase A (no DB writes): read everything from TalentLyft, mirror files to R2 ──
  const form = await tl<TlForm>(`/jobs/${job.Id}/form`);
  const coverField = form.FormFields.find((f) => f.Key === "CoverLetter");
  const questions = [...form.Questions].sort((a, b) => a.Position - b.Position);

  const summaries = (await tlAll<TlCandidateSummary>(`/candidates?jobs=${job.Id}`)).filter(
    (c) => ONLY_CANDIDATES.length === 0 || ONLY_CANDIDATES.includes(c.Id),
  );
  report.candidatesTotal = summaries.length;

  const mirror = APPLY && !SKIP_RESUMES;
  const plans = await mapPool(summaries, 4, async (c): Promise<CandidatePlan | { skip: string }> => {
    const app = c.Applications.find((a) => a.Job.Id === job.Id);
    const who = `${c.Id} ${`${c.FirstName ?? ""} ${c.LastName ?? ""}`.replace(/\s+/g, " ").trim()}`;
    if (!app) return { skip: `${who} (no application for this job)` };

    const realEmail = (c.Emails.find((e) => e.IsPrimary) ?? c.Emails[0])?.Value?.trim().toLowerCase();
    // OpenATS requires a unique (job, email); one shared address cannot be used twice, so keep one
    // shared domain with a per-candidate local part.
    const email = realEmail || `tl-${c.Id}@${MOCK_EMAIL_DOMAIN}`;

    const [detail, activities] = await Promise.all([
      tl<TlApplicationDetail>(`/candidates/${c.Id}/applications/${app.Id}`),
      tlAll<TlActivity>(`/candidates/${c.Id}/activities`),
    ]);
    const mine = activities.filter((a) => a.Application?.Id === app.Id || a.Job?.Id === job.Id);

    let resumeUrl: string | null = null;
    if (detail.Resume && mirror) resumeUrl = await mirrorFile(detail.Resume, `talentlyft-${app.Id}-resume`);
    if (resumeUrl) stats.resumesUploaded++;

    const answers: CandidatePlan["answers"] = [];
    for (const a of detail.Answers ?? []) {
      if (!a.Answer) continue;
      let text = a.Answer.Response?.trim() ?? "";
      if (!text && a.Answer.File) {
        if (mirror) {
          const url = await mirrorFile(a.Answer.File, `talentlyft-${app.Id}-q${a.Id}`);
          if (url) {
            stats.answerFilesUploaded++;
            text = url;
          }
        } else {
          text = `(file) ${a.Answer.File.FileName}`;
        }
      }
      if (text) answers.push({ tlQuestionId: a.Id, text: pg(text) });
    }

    const rejected = app.Disqualified;
    const theyLeft = (app.DisqualificationType ?? "").toLowerCase().includes("they");
    const stageName = mapStageName(app.Stage.Type);
    const status: CandidatePlan["status"] = rejected
      ? theyLeft
        ? "withdrawn"
        : "rejected"
      : app.Stage.Type === "Hired"
        ? "hired"
        : app.Stage.Type === "Offer"
          ? "offered"
          : "active";

    const appliedAt = new Date(app.Created);
    let stageSince = new Date(app.InStageSince ?? app.DisqualifiedAt ?? app.Created);

    // stage history from the real "moved to stage" events, collapsed to OpenATS stages
    const history: CandidatePlan["stageHistory"] = [{ stageName: "Screening", at: appliedAt }];
    for (const m of mine
      .filter((a) => a.ActivityTypeEnum === "MoveToStage" && a.Stage)
      .sort((a, b) => a.Created.localeCompare(b.Created))) {
      const name = mapStageName(m.Stage!.Type);
      if (history[history.length - 1]?.stageName !== name) history.push({ stageName: name, at: new Date(m.Created) });
    }
    if (!rejected && history[history.length - 1]?.stageName !== stageName) {
      history.push({ stageName, at: stageSince });
    }
    // TalentLyft often leaves InStageSince empty; the last real move is the better "entered stage" date.
    if (!rejected && !app.InStageSince) {
      const last = history[history.length - 1];
      if (last) stageSince = last.at;
    }

    const comments = mine
      .filter((a) => a.ActivityTypeEnum === "Comment" && a.Note?.Message)
      .sort((a, b) => a.Created.localeCompare(b.Created))
      .map((a) => ({
        at: new Date(a.Created),
        text: pg(`[TalentLyft · ${day(a.Created)} · ${a.Member?.Name ?? "System"}]\n${htmlToText(a.Note!.Message!)}`),
      }))
      .filter((n) => n.text.length > 0);

    // extra files (portfolio etc.) -> R2, listed in the import note
    const attachmentLines: string[] = [];
    for (const att of detail.Attachments ?? []) {
      let url: string | null = null;
      if (mirror) {
        url = await mirrorFile(att, `talentlyft-${app.Id}-att${att.Id ?? attachmentLines.length}`);
        if (url) stats.attachmentsUploaded++;
      }
      attachmentLines.push(`- ${att.FileName}${url ? `: ${url}` : mirror ? " (upload failed)" : ""}`);
    }

    const noteLines: string[] = [
      `[Imported from TalentLyft] candidate #${c.Id}, application #${app.Id}`,
      `Applied: ${day(app.Created)} · Source: ${app.Source?.SourceFormatted ?? c.Source?.SourceFormatted ?? "unknown"}`,
    ];
    if (!realEmail) noteLines.push("⚠ No email in TalentLyft: a placeholder address was generated, please collect the real one.");
    if (c.Tags?.length) noteLines.push(`Tags: ${c.Tags.join(", ")}`);
    if (rejected) {
      noteLines.push(
        `Disqualified${app.DisqualifiedAt ? ` on ${day(app.DisqualifiedAt)}` : ""}: ${app.DisqualificationReason ?? "(no reason)"}${app.DisqualificationType ? ` [${app.DisqualificationType}]` : ""}`,
      );
    }
    if (detail.Experiences?.length) {
      noteLines.push("", "Experience:");
      for (const e of detail.Experiences) {
        noteLines.push(
          `- ${e.Title ?? "?"} @ ${e.Company ?? "?"} (${day(e.From)} – ${e.Current ? "present" : day(e.To)})` +
            (e.Description?.trim() ? `\n  ${e.Description.trim().replace(/\r?\n/g, "\n  ")}` : ""),
        );
      }
    }
    if (detail.Educations?.length) {
      noteLines.push("", "Education:");
      for (const e of detail.Educations) {
        noteLines.push(
          `- ${e.Degree ?? ""} ${e.Institution ?? ""} (${day(e.From)} – ${e.Current ? "present" : day(e.To)})`.replace(/\s+/g, " "),
        );
      }
    }
    if (attachmentLines.length) noteLines.push("", "Additional files:", ...attachmentLines);

    return {
      tlCandidateId: c.Id,
      tlApplicationId: app.Id,
      firstName: (c.FirstName ?? "").trim().slice(0, 100) || "-",
      lastName: (c.LastName ?? "").trim().slice(0, 100) || "-",
      email,
      mockEmail: !realEmail,
      phone: (c.PhoneNumbers.find((p) => p.IsPrimary) ?? c.PhoneNumbers[0])?.Value?.trim().slice(0, 50) ?? null,
      resumeUrl,
      stageName,
      status,
      appliedAt,
      updatedAt: new Date(c.LastModified ?? app.Created),
      stageSince,
      stageHistory: history,
      rejection:
        status === "rejected" || status === "withdrawn"
          ? {
              reason: app.DisqualificationReason?.trim().slice(0, 255) || null,
              note: app.DisqualificationType ? `TalentLyft: ${app.DisqualificationType}` : null,
              at: new Date(app.DisqualifiedAt ?? app.InStageSince ?? app.Created),
            }
          : null,
      coverLetter: detail.CoverLetter?.trim() ? pg(detail.CoverLetter.trim()) : null,
      answers,
      comments,
      importNote: pg(noteLines.join("\n")),
      hadResumeFile: !!detail.Resume,
    };
  });

  // ── Phase B: one transaction per job ──
  try {
    await db.transaction(async (tx) => {
      // department
      const deptName = (job.Department?.Name ?? "General").trim().slice(0, 255) || "General";
      let [dept] = await tx
        .select()
        .from(departments)
        .where(sql`${departments.companyId} = ${ctx.companyId} and lower(${departments.name}) = ${deptName.toLowerCase()}`);
      if (!dept) {
        [dept] = await tx.insert(departments).values({ companyId: ctx.companyId, name: deptName }).returning();
        report.departmentCreated = true;
      }
      if (!dept) throw new Error("Failed to resolve department");
      report.department = dept.name;

      const emp = mapEmploymentType(job.Title, job.Contract?.Text, dept.name);
      report.employmentType = emp.value;
      report.employmentTypeSource = emp.source;

      const description = [job.Description, job.Requirements ? `<h3>Requirements</h3>${job.Requirements}` : null]
        .filter(Boolean)
        .join("\n");

      const [newJob] = await tx
        .insert(jobs)
        .values({
          title: job.Title.trim().slice(0, 255),
          slug,
          departmentId: dept.id,
          employmentType: emp.value,
          location: job.Location?.CityCountry?.trim().slice(0, 255) || null,
          description: description ? pg(sanitizeHtml(description)) : null,
          status: "published",
          createdBy: ctx.createdBy,
        })
        .returning();
      if (!newJob) throw new Error("Failed to insert job");
      report.openatsJobId = newJob.id;

      // pipeline stages from the default templates (same as jobService.create)
      const stageRows = await tx
        .insert(jobPipelineStages)
        .values(
          ctx.templates.map((t) => ({
            jobId: newJob.id,
            name: t.name,
            position: t.position,
            stageType: t.stageType,
            sourceTemplateId: t.id,
          })),
        )
        .returning();
      const stageIdByName = new Map(stageRows.map((s) => [s.name, s.id]));
      await tx.insert(jobHiringTeam).values({ jobId: newJob.id, userId: ctx.createdBy });

      // questions: cover letter first (it is part of the TalentLyft application form), then screening questions
      const questionIdByTl = new Map<number, number>();
      let coverQuestionId: number | null = null;
      let position = 1;
      if (coverField) {
        const [q] = await tx
          .insert(jobCustomQuestions)
          .values({
            jobId: newJob.id,
            title: pg(coverField.DisplayName).slice(0, 500),
            questionType: "long_answer",
            isRequired: coverField.Required,
            position: position++,
          })
          .returning();
        coverQuestionId = q?.id ?? null;
        report.questions++;
      }
      for (const q of questions) {
        const type = mapQuestionType(q.Type);
        if (!type) {
          report.unsupportedQuestions.push(`${q.Type}: ${q.DisplayName}`);
          continue;
        }
        const fromFile = q.Type === "file";
        const [row] = await tx
          .insert(jobCustomQuestions)
          .values({
            jobId: newJob.id,
            title: pg(fromFile ? `${q.DisplayName} (ไฟล์แนบ)` : q.DisplayName).slice(0, 500),
            questionType: type,
            isRequired: q.Required,
            position: position++,
          })
          .returning();
        if (row) {
          questionIdByTl.set(q.Id, row.id);
          report.questions++;
          if (fromFile) report.questionsFromFileType++;
        }
      }

      // candidates
      const seenEmails = new Set<string>();
      for (const plan of plans) {
        if ("skip" in plan) {
          report.skipped.push(plan.skip);
          continue;
        }
        if (seenEmails.has(plan.email)) {
          report.duplicatesSkipped.push(`${plan.email} (application ${plan.tlApplicationId})`);
          continue;
        }
        seenEmails.add(plan.email);

        const screeningId = stageIdByName.get("Screening");
        const currentId = stageIdByName.get(plan.stageName) ?? screeningId;
        if (!screeningId || !currentId) throw new Error("Default pipeline stages are missing (run the seed)");

        const rejected = plan.status === "rejected" || plan.status === "withdrawn";
        const [cand] = await tx
          .insert(candidates)
          .values({
            firstName: plan.firstName,
            lastName: plan.lastName,
            email: plan.email,
            phone: plan.phone,
            resumeUrl: plan.resumeUrl,
            jobId: newJob.id,
            currentStageId: rejected ? null : currentId,
            status: plan.status,
            appliedAt: plan.appliedAt,
            updatedAt: plan.updatedAt,
          })
          .returning();
        if (!cand) throw new Error("Failed to insert candidate");

        await tx.insert(candidateStageHistory).values(
          plan.stageHistory.map((h) => ({
            candidateId: cand.id,
            stageId: stageIdByName.get(h.stageName) ?? screeningId,
            movedAt: h.at,
          })),
        );

        if (plan.rejection) {
          const lastStage = plan.stageHistory[plan.stageHistory.length - 1]?.stageName;
          await tx.insert(candidateRejections).values({
            candidateId: cand.id,
            jobId: newJob.id,
            fromStageId: (lastStage && stageIdByName.get(lastStage)) || currentId,
            reason: plan.rejection.reason,
            internalNote: plan.rejection.note,
            emailStatus: "not_sent",
            rejectedAt: plan.rejection.at,
          });
        }

        // The app creates a draft offer whenever a candidate enters an Offer-type stage (Offer, Hired).
        if (!rejected && (plan.stageName === "Offer" || plan.stageName === "Hired")) {
          const [offer] = await tx
            .insert(offers)
            .values({ candidateId: cand.id, jobId: newJob.id, status: "draft", createdBy: ctx.createdBy, createdAt: plan.stageSince })
            .returning();
          if (offer) {
            await tx.insert(candidateActivities).values({
              candidateId: cand.id,
              jobId: newJob.id,
              offerId: offer.id,
              stageId: currentId,
              actorId: ctx.createdBy,
              eventType: "offer_created",
              createdAt: plan.stageSince,
            });
            report.offersCreated++;
          }
        }

        const answerRows: { candidateId: number; questionId: number; answerText: string }[] = [];
        if (coverQuestionId && plan.coverLetter) {
          answerRows.push({ candidateId: cand.id, questionId: coverQuestionId, answerText: plan.coverLetter });
        }
        for (const a of plan.answers) {
          const qid = questionIdByTl.get(a.tlQuestionId);
          if (qid) answerRows.push({ candidateId: cand.id, questionId: qid, answerText: a.text });
        }
        if (answerRows.length > 0) await tx.insert(candidateCustomAnswers).values(answerRows);

        // internal notes: the import summary first, then the recruiters' original comments
        await tx.insert(candidateChatMessages).values([
          { candidateId: cand.id, senderId: ctx.createdBy, message: plan.importNote, sentAt: plan.appliedAt, isSystemMessage: true },
          ...plan.comments.map((n) => ({
            candidateId: cand.id,
            senderId: ctx.createdBy,
            message: n.text,
            sentAt: n.at,
            isSystemMessage: false,
          })),
        ]);
        report.commentsMigrated += plan.comments.length;

        report.candidatesInserted++;
        report.byStage[plan.stageName] = (report.byStage[plan.stageName] ?? 0) + 1;
        report.byStatus[plan.status] = (report.byStatus[plan.status] ?? 0) + 1;
        if (plan.rejection?.note) {
          report.rejectionTypes[plan.rejection.note] = (report.rejectionTypes[plan.rejection.note] ?? 0) + 1;
        }
        if (plan.mockEmail) report.mockEmails.push(`${plan.tlCandidateId} ${plan.firstName} ${plan.lastName} -> ${plan.email}`);
        if (!plan.hadResumeFile) report.withoutResume++;
      }

      if (!APPLY) throw new Rollback();
    });
  } catch (err) {
    if (!(err instanceof Rollback)) {
      report.status = "failed";
      const cause = err instanceof Error && err.cause instanceof Error ? ` | cause: ${err.cause.message}` : "";
      report.error = `${err instanceof Error ? err.message.split("\n")[0] : String(err)}${cause}`;
    }
  }
  return report;
}

// ───────────────────────── main ─────────────────────────

async function main() {
  const need = ["TALENTLYFT_API_KEY", "DATABASE_URL"];
  if (APPLY && !SKIP_RESUMES) need.push("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME", "R2_PUBLIC_URL");
  const missing = need.filter((k) => !process.env[k]);
  if (missing.length) throw new Error(`Missing env: ${missing.join(", ")}`);
  if (APPLY && !CREATED_BY_EMAIL) throw new Error("--created-by=<email> is required with --apply");

  console.log(APPLY ? "MODE: APPLY (writing)" : "MODE: DRY RUN (every job transaction is rolled back, no R2 uploads)");

  const [co] = await db.select().from(company).limit(1);
  if (!co) throw new Error("No company row exists in OpenATS; complete company setup first");

  const allUsers = await db.select().from(users).where(eq(users.isActive, true)).orderBy(asc(users.id));
  const owner = CREATED_BY_EMAIL
    ? allUsers.find((u) => u.email.toLowerCase() === CREATED_BY_EMAIL.toLowerCase())
    : allUsers[0];
  if (!owner) throw new Error(CREATED_BY_EMAIL ? `No active user with email ${CREATED_BY_EMAIL}` : "No active users exist yet; sign in once first");
  console.log(`Owner: ${owner.email} (id ${owner.id}); company: ${co.name}`);

  const templates = await db.select().from(pipelineStageTemplates).orderBy(asc(pipelineStageTemplates.position));
  if (templates.length === 0) throw new Error("pipeline_stage_templates is empty (run the seed)");

  let tlJobs = await tlAll<TlJob>("/jobs?status=Published&details=true");
  tlJobs = tlJobs.filter((j) => j.Status === "Published" && (ONLY_JOBS.length === 0 || ONLY_JOBS.includes(j.Id)));
  console.log(`Published TalentLyft jobs to migrate: ${tlJobs.length}`);

  const reports: JobReport[] = [];
  for (const job of tlJobs) {
    console.log(`\n→ ${job.Id} ${job.Title}`);
    const r = await migrateJob(job, { createdBy: owner.id, companyId: co.id, templates });
    reports.push(r);
    console.log(
      `  ${r.status}${r.error ? `: ${r.error}` : ""} | candidates ${r.candidatesInserted}/${r.candidatesTotal} | questions ${r.questions} | type ${r.employmentType ?? "-"}`,
    );
  }

  const summary = { mode: APPLY ? "apply" : "dry-run", reports, files: stats };
  console.log("\n" + JSON.stringify(summary, null, 2));
  if (REPORT_PATH) fs.writeFileSync(REPORT_PATH, JSON.stringify(summary, null, 2));
  if (reports.some((r) => r.status === "failed")) process.exitCode = 1;
}

main()
  .catch((err) => {
    console.error("Migration failed:", err);
    process.exitCode = 1;
  })
  .finally(() => process.exit());
