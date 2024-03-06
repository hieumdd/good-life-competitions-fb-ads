import { Storage } from '@google-cloud/storage';

export const storageClient = new Storage();

export const createWriteStream = (bucketName: string, fileName: string) => {
    return storageClient.bucket(bucketName).file(fileName).createWriteStream();
};
