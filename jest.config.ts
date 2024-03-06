export default {
    testEnvironment: 'node',
    testRegex: './src/.*.test.ts$',
    setupFiles: ['dotenv/config'],
    testTimeout: 540000,
    transform: {
        '^.+\\.(t|j)sx?$': '@swc/jest',
    },
};
