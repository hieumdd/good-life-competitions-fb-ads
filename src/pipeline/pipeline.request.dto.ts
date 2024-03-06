import Joi from 'joi';
import { ContainerTypes, ValidatedRequestSchema } from 'express-joi-validation';

import dayjs from '../dayjs';
import * as pipelines from './pipeline.const';

export type FacebookRequestOptions = {
    accountId: string;
    start: string;
    end: string;
};

export type CreatePipelineTasksBody = Partial<Omit<FacebookRequestOptions, 'accountId'>>;

export interface CreatePipelineTasksRequest extends ValidatedRequestSchema {
    [ContainerTypes.Body]: CreatePipelineTasksBody;
}

export const CreatePipelineTasksBodySchema = Joi.object<CreatePipelineTasksBody>({
    start: Joi.string().optional().empty(null).allow(null).default(dayjs.utc().subtract(7, 'day').format('YYYY-MM-DD')),
    end: Joi.string().optional().empty(null).allow(null).default(dayjs.utc().format('YYYY-MM-DD')),
});

export type RunInsightsPipelineBody = FacebookRequestOptions & {
    pipeline: keyof typeof pipelines;
};

export interface RunInsightsPipelineRequest extends ValidatedRequestSchema {
    [ContainerTypes.Body]: RunInsightsPipelineBody;
}

export const RunInsightsPipelineBodySchema = Joi.object<RunInsightsPipelineBody>({
    accountId: Joi.string().required(),
    start: Joi.string().required(),
    end: Joi.string().required(),
    pipeline: Joi.string()
        .valid(...Object.keys(pipelines))
        .required(),
});
