require('dotenv').config()
const { Sequelize, UUIDV4 } = require('sequelize');
const { 
  S3Client, 
  ListObjectsV2Command, 
  CopyObjectCommand 
 } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');

const fileServerDB = new Sequelize(process.env.FILE_SERVER_DB_NAME, process.env.DB_USERNAME, process.env.DB_PASSWORD, {
  host: process.env.DB_HOST,
  dialect: 'postgres'
});

const migrateS3Data = async () => {
  const s3Client = new S3Client({
    region: process.env.AWS_REGION
  })

  let now = new Date();

  const input = {
    Bucket: process.env.AWS_SOURCE_BUCKET,
    Prefix: 'qa/fifo/images/',
    MaxKeys: 1000,
  };

  let isTruncated = true;
  let s3Keys = [];
  let fileKeys = [];

  while (isTruncated) {
    const command = new ListObjectsV2Command(input);

    const response = await s3Client.send(command);

    const contents = response.Contents;
    console.log(`Total files: ${response.Contents.length}`);

    isTruncated = response.IsTruncated;
    command.input.ContinuationToken = response.NextContinuationToken;

    for (let i = 0; i < contents.length; i++) {
      const wholeKey = response.Contents[i].Key;
      const splitKey = wholeKey.split('qa/fifo/images/');
      const objectKey = splitKey[1];

      // const copyCommand = new CopyObjectCommand({
      //   CopySource: `${process.env.AWS_SOURCE_BUCKET}/${wholeKey}`,
      //   Bucket: process.env.AWS_DESTINATION_BUCKET,
      //   Key: wholeKey,
      // });

      // await s3Client.send(copyCommand);
      s3Keys.push(wholeKey);
      console.log(`Migration to new bucket Done -> ${objectKey}`)
    }
  }

  for (let i = 0; i < s3Keys.length; i++) {
    let objectKey = s3Keys[i];

    let requiredKey;
    let requierdQuality = [];
    let smallFlag = false;
    let mediumFlag = false;
    let largeFlag = false;
    let originalFlag = false

    if (objectKey.includes(`_small`)) {
      await modifyImageArray('small', objectKey, fileKeys, requierdQuality, requiredKey, smallFlag);
    } else if (objectKey.includes(`_medium`)) {
      await modifyImageArray('medium', objectKey, fileKeys, requierdQuality, requiredKey, mediumFlag);
    } else if (objectKey.includes(`_large`)) {
      await modifyImageArray('large', objectKey, fileKeys, requierdQuality, requiredKey, largeFlag);
    } else {
      await modifyImageArray(null, objectKey, fileKeys, requierdQuality, requiredKey, originalFlag);
    }
  }

  await Promise.all(fileKeys.map(async (filekey) => {
    await fileServerDB.query(`insert into images (id, image_key, quality, privacy, client, created_at, updated_at) values ($id, $imageKey, $quality, $privacy, $client, $createdAt, $updatedAt)`, {
      bind: {
        id: uuidv4(),
        imageKey: filekey.key,
        quality: filekey.quality,
        privacy: 'public',
        client: 'fifo',
        createdAt: now,
        updatedAt: now,
      }
    })
  }))

  console.log(fileKeys)
}


const modifyImageArray = (fileType, objectKey, fileKeys, requierdQuality, requiredKey, flag) => {
  let qualityKey;

  if (fileType) {
    qualityKey = `_${fileType}`
  } else {
    qualityKey = null;
  }

  if (fileKeys.length <1) {
    requierdQuality.push(fileType)
    requiredKey = {
      key: objectKey.replace(qualityKey, ''),
      quality: requierdQuality
    }

    fileKeys.push(requiredKey);
  } else {
    for (let i = 0; i < fileKeys.length; i++) {
      if (objectKey.replace(qualityKey, '') === fileKeys[i].key) {
        fileKeys[i].quality.push(fileType)
        flag = true;
      }
    }

    if (!flag) {
      requierdQuality.push(fileType)
      requiredKey = {
        key: objectKey.replace(qualityKey, ''),
        quality: requierdQuality
      }

      fileKeys.push(requiredKey);
    }
  }
}

migrateS3Data();