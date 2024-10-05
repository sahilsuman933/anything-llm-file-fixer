require('dotenv').config();

const { PrismaClient } = require('@prisma/client');
const AWS = require('aws-sdk');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const winston = require('winston');
const { Logtail } = require('@logtail/node');
const { LogtailTransport } = require('@logtail/winston');

const prisma = new PrismaClient();

AWS.config.update({
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const s3 = new AWS.S3();
const textract = new AWS.Textract();

const logtail = new Logtail(process.env.BROKENFILE_BETTER_STACK);
const logger = winston.createLogger({
  level: 'info',
  transports: [
    new LogtailTransport(logtail),
    new winston.transports.Console(),
  ],
});

function parseS3Url(url) {
  if (url.startsWith('s3://')) {
    const urlWithoutProtocol = url.slice(5);
    const [bucket, ...keyParts] = urlWithoutProtocol.split('/');
    const key = keyParts.join('/');
    return { bucket, key };
  } else {
    const parsedUrl = new URL(url);
    let bucket = '';
    let key = '';
    if (parsedUrl.hostname.endsWith('amazonaws.com')) {
      const parts = parsedUrl.hostname.split('.');
      bucket = parts[0];
      key = parsedUrl.pathname.slice(1);
    } else {
      bucket = parsedUrl.hostname;
      key = parsedUrl.pathname.slice(1);
    }

    key = decodeURIComponent(key);
    return { bucket, key };
  }
}

function tokenizeString(content) {
  const tokens = content.match(/\b\w+\b/g);
  return tokens || [];
}

async function processFile(file) {
  try {
    logger.info(`Processing file: ${file.title}`);

    const { bucket, key } = parseS3Url(file.url);
    const s3Params = {
      Bucket: process.env.S3_BUCKET_NAME,
      Key: key,
    };
    const s3Data = await s3.getObject(s3Params).promise();
    const fileData = s3Data.Body;
    const fileSize = s3Data.ContentLength;
    logger.info(`Downloaded file from S3: ${file.url}, size: ${fileSize} bytes`);

    const originalFileName = path.basename(key);
    const fileNameWithoutExt = originalFileName.replace(path.extname(originalFileName), '');

    if (fileSize > 5 * 1024 * 1024) {
      logger.error(`File ${file.id} exceeds the 5 MB limit for synchronous Textract operations.`);
      return;
    }

    const textractParams = {
      Document: {
        Bytes: fileData,
      },
    };
    const textractResponse = await textract.detectDocumentText(textractParams).promise();
    const textBlocks = textractResponse.Blocks.filter((block) => block.BlockType === 'LINE');
    const content = textBlocks.map((block) => block.Text).join('\n');
    logger.info(`Extracted text from file using Textract`);

    const textFileKey = `pageContents/${fileNameWithoutExt}.txt`;
    const uploadParams = {
      Bucket: process.env.S3_BUCKET_NAME,
      Key: textFileKey,
      Body: content,
      ContentType: 'text/plain',
    };
    await s3.putObject(uploadParams).promise();

    const pageContentUploadUrl = `https://${uploadParams.Bucket}.s3.${process.env.AWS_REGION}.amazonaws.com/${uploadParams.Key}`;
    logger.info(`Uploaded text file to S3: ${pageContentUploadUrl}`);

    await prisma.file.update({
      where: { id: file.id },
      data: {
        pageContentUrl: pageContentUploadUrl,
        wordCount: content.split(' ').length,
        tokenCountEstimate: tokenizeString(content).length,
      },
    });
    logger.info(`Updated file record in database: ${file.id}`);
  } catch (error) {
    logger.error(`Error processing file ${file.id}: ${error.message}`);
  }
}

async function main() {
  try {
    const files = await prisma.file.findMany({
      where: { pageContentUrl: null },
    });

    logger.info(`Found ${files.length} files to process`);

    for (const file of files) {
      await processFile(file);
    }

    logger.info(`Processing complete`);
  } catch (error) {
    logger.error(`Error in main: ${error.message}`);
  } finally {
    await prisma.$disconnect();
  }
}

main();
