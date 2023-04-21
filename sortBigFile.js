import { createReadStream, createWriteStream } from 'fs';
import { rm } from 'fs/promises';
import { pipeline } from 'stream/promises';
import readline from 'readline';

const BufferSize = 10_000_000; // размер буфера, загружаемый в память
const MaxMemoryUse = 10_000_000; // размер памяти, который мы можем использовать

async function externSort(fileName) {
  const file = createReadStream(fileName, { highWaterMark: BufferSize }); // параметр - размер буфера, который мы загружаем из файла в память
  const lines = readline.createInterface({ input: file, crlfDelay: Infinity }); // последний параметр нужен для того, чтобы \n считывалось как разделитель строк
  const chunk = [];
  let size = 0;
  const tmpFileNames = [];
  for await (let line of lines) {
    size += line.length;
    chunk.push(line);
    if (size > MaxMemoryUse) {
      await sortAndWriteToFile(chunk, tmpFileNames);
      size = 0;
    }
  }
  if (chunk.length > 0) {
    await sortAndWriteToFile(chunk, tmpFileNames);
  }
  await merge(tmpFileNames, fileName);
  await cleanUp(tmpFileNames);
}

async function merge(tmpFileNames, fileName) {
  console.log('merging result ...');
  const resultFileName = `${fileName.split('.txt')[0]}-sorted.txt`;
  const file = createWriteStream(resultFileName, { highWaterMark: BufferSize });
  const activeReaders = tmpFileNames.map( // создаем объекты в котором у нас задан поток для чтения и асинхронный итератор
    name => readline.createInterface(
      { input: createReadStream(name, { highWaterMark: BufferSize }), crlfDelay: Infinity }
    )[Symbol.asyncIterator]()
  );
  const values = await Promise.all(activeReaders.map(r => r.next().then(e => e.value))); // минимальные значения из всех временных файлов
  return pipeline(
    async function* () { 
      while (activeReaders.length > 0) {                                       
        const [minVal, i] = values.reduce((prev, cur, idx) => cur < prev ? [cur, idx] : prev, ['z'.repeat(36), -1]); // находим минимальное значение из всех временных файлов
        yield `${minVal}\n`; // передаем минимальное значение в следующее значение итератора
        const res = await activeReaders[i].next(); // из того файла, в котором мы взяли минимальное значение берем следующее за ним значение
        if (!res.done) { 
          values[i] = res.value;
        } else { // если файл закончился, убираем из массива с минимальными значениями его элементы и из перебираемых объектов тоже 
          values.splice(i, 1); 
          activeReaders.splice(i, 1);
        }
      }
    },
    file
  );
}

async function sortAndWriteToFile(chunk, tmpFileNames) {
  chunk.sort();
  let tmpFileName = `tmp_sort_${tmpFileNames.length}.txt`;
  tmpFileNames.push(tmpFileName);
  console.log(`creating tmp file: ${tmpFileName}`);
  await pipeline( 
    chunk.map(e => `${e}\n`),
    createWriteStream(tmpFileName, { highWaterMark: BufferSize })
  );
  chunk.length = 0; // отчищаем память
}

function cleanUp(tmpFileNames) {
    return Promise.all(tmpFileNames.map(f => rm(f)));
  }


const fileName = 'big_file.txt';
await externSort(fileName);
