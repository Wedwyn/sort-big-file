import fs from  'fs';
import {v4 as uuidv4} from 'uuid';

let myuuid = uuidv4();

function write(fileName, countOfRecords) {
    const writableStream = fs.createWriteStream(fileName);

    writableStream.on('error',  (error) => {
        console.log(`An error occured while writing to the file. Error: ${error.message}`);
    });

    for (let i = 0; i < countOfRecords; i += 1) {
        const uuid = uuidv4();
        writableStream.write(`${uuid}\n`);
    }
    console.log(`create file ${fileName} with ${countOfRecords} uuid records`)
}

write('big_file.txt', 5_000_000); // 185 MB