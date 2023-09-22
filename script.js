require('dotenv').config()
const { Sequelize, UUIDV4 } = require('sequelize');
const { 
  S3Client, 
  ListObjectsV2Command, 
  CopyObjectCommand 
 } = require('@aws-sdk/client-s3');
 const {mapLimit} = require('awaity');
const { v4: uuidv4 } = require('uuid');

const fileServerDB = new Sequelize(process.env.FILE_SERVER_DB_NAME, process.env.DB_USERNAME, process.env.DB_PASSWORD, {
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  dialect: 'postgres'
});

const migrateS3Data = async () => {
  const s3Client = new S3Client({
    region: process.env.AWS_REGION
  })

  let now = new Date();

  let s3Keys = [];
  let fileKeys = [];
  
  let smallRE = new RegExp('small$');
  let mediumRE = new RegExp('medium$');
  let largeRE = new RegExp('large$');

  let path = ['profile_pics/', 'cover_pics/', 'qa/fifo/images/'];

  for (let j = 0; j < path.length; j++) {
    let isTruncated = true;

    const input = {
      Bucket: process.env.AWS_SOURCE_BUCKET,
      Prefix: path[j],
      MaxKeys: 1000,
    };
  
    while (isTruncated) {
      const command = new ListObjectsV2Command(input);
  
      const response = await s3Client.send(command);
  
      isTruncated = response.IsTruncated;
      command.input.ContinuationToken = response.NextContinuationToken;

      const promises = []
      if (response.KeyCount > 0) {
        const contents = response.Contents;
        console.log(`Total files: ${response.Contents.length}`);
    
        for (let i = 0; i < contents.length; i++) {
          const wholeKey = response.Contents[i].Key;
          const splitKey = wholeKey.split(path[j]);
          const objectKey = splitKey[1];
    
          const copyCommand = new CopyObjectCommand({
            CopySource: `${process.env.AWS_SOURCE_BUCKET}/${wholeKey}`,
            Bucket: process.env.AWS_DESTINATION_BUCKET,
            Key: wholeKey,
          });
    
          promises.push(s3Client.send(copyCommand));
          s3Keys.push(wholeKey);
          console.log(`Migration to new bucket Done -> ${objectKey}`)
        }
        await Promise.all(promises)
      }
    }
  }

  console.log(`Creating Array of S3 objects : ${s3Keys.length}`);

  for (let i = 0; i < s3Keys.length; i++) {
    let objectKey = s3Keys[i];

    if (smallRE.test(objectKey)) {
      await modifyImageArray('small', objectKey, fileKeys);
    } else if (mediumRE.test(objectKey)) {
      await modifyImageArray('medium', objectKey, fileKeys);
    } else if (largeRE.test(objectKey)) {
      await modifyImageArray('large', objectKey, fileKeys);
    } else {
      await modifyImageArray(null, objectKey, fileKeys);
    }
  }

  console.log('Saving into file server DB');

  await mapLimit(fileKeys, async (filekey) => {
    return fileServerDB.query(`insert into images (id, image_key, quality, privacy, client, created_at, updated_at) values ($id, $imageKey, $quality, $privacy, $client, $createdAt, $updatedAt)`, {
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
  }, 10)

  console.log('Migration Completed and saved to DB')
}


const modifyImageArray = (fileType, objectKey, fileKeys) => {
  let qualityKey;
  let flag = false;
  let requiredKey;
  let requierdQuality = [];


  if (fileType) {
    qualityKey = `-${fileType}`
  } else {
    qualityKey = null;
  }

  if (fileKeys.length <1) {
    if (fileType) {
      requierdQuality.push(fileType)
    }

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
      if (fileType) {
        requierdQuality.push(fileType)
      }

      requiredKey = {
        key: objectKey.replace(qualityKey, ''),
        quality: requierdQuality
      }

      fileKeys.push(requiredKey);
    }
  }
}

migrateS3Data();