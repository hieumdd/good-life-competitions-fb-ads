import { Readable, Transform, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';
import Joi from 'joi';

import dayjs from '../dayjs';
import { createWriteStream } from '../google-cloud/storage.service';
import { FacebookRequestOptions } from './pipeline.request.dto';

const validateTransform = (schema: Joi.Schema) => {
    return async function* (rows: any) {
        for await (const row of rows) {
            const value = await schema.validateAsync(row);
            yield { ...value, _batched_at: dayjs().utc().toISOString() };
        }
    };
};

export type RunPipelineOptions = FacebookRequestOptions & { bucketName: string };

type CreateInsightsPipelineConfig = {
    name: string;
    extractStream: (options: FacebookRequestOptions) => Promise<Readable>;
    validationSchema: Joi.Schema;
    schema: any[];
};

export const createInsightsPipeline = (options: CreateInsightsPipelineConfig) => {
    const { name, extractStream, validationSchema, schema } = options;

    const standardize = validateTransform(validationSchema);

    const grouping = async function* (rows: any) {
        const state: Record<string, object[]> = {};
        for await (const row of rows) {
            state[row.date_start] = [...(state[row.date_start] ?? []), row];
        }
        for (const entry of Object.entries(state)) {
            yield entry;
        }
    };

    const partitionedWrite = (bucketName: string, fileName: (key: string) => string) => {
        return new Writable({
            objectMode: true,
            write: ([key, rows], _, callback) => {
                pipeline(Readable.from(rows), ndjson.stringify(), createWriteStream(bucketName, fileName(key)))
                    .then(() => callback())
                    .catch((error) => callback(error));
            },
        });
    };

    const run = async (options: RunPipelineOptions) => {
        const sourceStream = await extractStream(options);
        const writeStream = partitionedWrite(options.bucketName, (key) => {
            const { accountId } = options;
            return `insights/${name}/_account_id=${accountId}/_date_start=${key}/data.json`;
        });
        return await pipeline(sourceStream, standardize, grouping, writeStream);
    };

    return { name, schema, run };
};
