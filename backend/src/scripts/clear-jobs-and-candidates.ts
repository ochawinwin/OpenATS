import "dotenv/config";
import { count } from "drizzle-orm";
import { DeleteObjectCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { db } from "../db";
import { candidates, jobs, offers } from "../db/schema";
import { jobService } from "../modules/job/job.service";
import { resumeDeleteQueue } from "../queues/resume-delete/queue";

/**
 * One-off: delete EVERY job and candidate (with their offers, interviews, notes, chat, emails,
 * assessments and CV files) so the database is empty before a fresh import. Users, settings,
 * templates, departments and the default pipeline stages are not touched.
 *
 *   node dist/src/scripts/clear-jobs-and-candidates.js                                  # dry run: only counts
 *   node dist/src/scripts/clear-jobs-and-candidates.js --apply --expect-jobs=<n>         # delete for real
 *   ... --purge-public-resumes   also erase `resumes/` from the old PUBLIC bucket (R2_BUCKET_NAME):
 *                                CVs saved there before the private bucket existed, which the
 *                                resume-delete queue does not reach
 *
 * It goes through jobService.deleteMany, the same code as the dashboard's Delete, so CV files in the
 * private bucket are queued for deletion after the commit (the worker must be running).
 * `--expect-jobs` must equal the number of jobs found, so a script pointed at the wrong database or
 * run after more jobs were added refuses instead of deleting.
 */
const argv = process.argv.slice(2);
const APPLY = argv.includes("--apply");
const PURGE_PUBLIC = argv.includes("--purge-public-resumes");
const EXPECT = argv.find((a) => a.startsWith("--expect-jobs="))?.split("=")[1];

async function purgePublicResumes() {
  const bucket = process.env.R2_BUCKET_NAME;
  const privateBucket = process.env.R2_PRIVATE_BUCKET_NAME;
  if (!bucket) throw new Error("R2_BUCKET_NAME is not set");
  if (bucket === privateBucket) throw new Error("R2_BUCKET_NAME and R2_PRIVATE_BUCKET_NAME are the same bucket; refusing");

  const client = new S3Client({
    region: "us-east-1",
    endpoint: process.env.R2_ENDPOINT!,
    credentials: {
      accessKeyId: process.env.R2_ACCESS_KEY_ID!,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
    },
    forcePathStyle: true,
  });

  let deletedFiles = 0;
  let token: string | undefined;
  do {
    const page = await client.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: "resumes/", ContinuationToken: token }),
    );
    for (const { Key: key } of page.Contents ?? []) {
      if (!key) continue;
      await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
      deletedFiles++;
    }
    token = page.IsTruncated ? page.NextContinuationToken : undefined;
  } while (token);
  console.log(`Erased ${deletedFiles} file(s) under resumes/ from the public bucket ${bucket}.`);
}

async function main() {
  const [{ value: jobCount = 0 } = { value: 0 }] = await db.select({ value: count() }).from(jobs);
  const [{ value: candidateCount = 0 } = { value: 0 }] = await db.select({ value: count() }).from(candidates);
  const [{ value: offerCount = 0 } = { value: 0 }] = await db.select({ value: count() }).from(offers);
  console.log(`Found ${jobCount} job(s), ${candidateCount} candidate(s), ${offerCount} offer(s).`);

  if (!APPLY) {
    console.log("Dry run: nothing was deleted. Add --apply --expect-jobs=" + jobCount + " to delete.");
    return;
  }
  if (EXPECT === undefined || Number(EXPECT) !== jobCount) {
    throw new Error(`Refusing: --expect-jobs=${EXPECT ?? "(missing)"} but the database has ${jobCount} job(s).`);
  }

  const ids = (await db.select({ id: jobs.id }).from(jobs)).map((row) => row.id);
  const CHUNK = 20;
  let removed = 0;
  for (let i = 0; i < ids.length; i += CHUNK) {
    const deleted = await jobService.deleteMany(ids.slice(i, i + CHUNK));
    removed += deleted.length;
    console.log(`  deleted ${removed}/${ids.length} job(s)`);
  }

  const [{ value: left = 0 } = { value: 0 }] = await db.select({ value: count() }).from(candidates);
  console.log(`Done: ${removed} job(s) deleted, ${left} candidate row(s) left.`);

  if (PURGE_PUBLIC) await purgePublicResumes();
}

main()
  .then(async () => {
    await resumeDeleteQueue.close();
    process.exit(0);
  })
  .catch(async (error) => {
    console.error(error instanceof Error ? error.message : error);
    await resumeDeleteQueue.close().catch(() => undefined);
    process.exit(1);
  });
