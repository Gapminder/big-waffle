/**
 * This is the main module that serves HTTP requests to BigWaffle.
 * 
 */
const Compress = require('koa-compress');
const Koa = require('koa');
const Router = require('koa-router');
const Moment = require('moment');

const { Dataset, DataSource } = require("./datasets");
const { HTTPPort } = require("./env");

const app = new Koa();
const api = new Router();   //routes for the main API
const admin = new Router({  //routes to manage datasets go here
  prefix: '/datasets'
});

api.get('/', async (ctx, next) => {
  /*
   * List all (public) datasets that are currently available.
   */
  const datasets = await Dataset.all();
  ctx.body = datasets.map(ds => ds.name);
});

api.get('/:dataset', async (ctx, next) => {
  const start = Moment();
  const key = ctx.query.key;
  const values = ctx.query.values.split(',').map(v => v.trim());
  //TODO: parse and validate all of the params
  const dataset = new DataSource(ctx.params.dataset);
  await dataset.open();
  const resultStream = await dataset.queryStream(key, values, start);
  ctx.type = 'application/json';
  ctx.body = resultStream;
})

app.use(Compress());
app.use(api.routes());
app.listen(HTTPPort);