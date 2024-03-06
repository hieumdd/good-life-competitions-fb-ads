import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import { getLogger } from '../logging.service';
import { QUEUE_CONFIG, getBucketName } from '../config';
import { createTasks } from '../google-cloud/cloud-tasks.service';
import { createWriteStream } from '../google-cloud/storage.service';
import { getAccounts } from '../facebook/account.service';
import { CreatePipelineTasksBody, FacebookRequestOptions } from './pipeline.request.dto';
import { RunPipelineOptions } from './pipeline.utils';
import * as pipelines from './pipeline.const';

const logger = getLogger(__filename);

type RunInsightsPipelineConfig = {
    name: string;
    run: (options: RunPipelineOptions) => Promise<any>;
};

export const runInsightsPipeline = async (
    { name, run }: Omit<RunInsightsPipelineConfig, 'bucketName'>,
    options: FacebookRequestOptions,
) => {
    logger.info(`Running insights pipeline ${name}`, options);

    const bucketName = await getBucketName();

    return await run({ ...options, bucketName }).then(() => options);
};

export const createInsightsPipelineTasks = async ({ start, end }: CreatePipelineTasksBody) => {
    logger.info('Creating insights pipeline tasks', { start, end });

    const bucketName = await getBucketName();
    const accounts = await Promise.all([618162358531378, 2030842403626659].map(getAccounts)).then((x) => x.flat());

    return await Promise.all([
        Object.keys(pipelines)
            .map((pipeline) => {
                return accounts.map(({ account_id }) => ({
                    accountId: account_id,
                    start,
                    end,
                    pipeline,
                }));
            })
            .map((data) => {
                return createTasks(QUEUE_CONFIG, data, (task) => {
                    return [task.pipeline, task.accountId].join('-');
                });
            }),
        pipeline(Readable.from(accounts), ndjson.stringify(), createWriteStream(bucketName, 'accounts.json')),
    ]).then(() => accounts.length);
};
