const { app } = require('@azure/functions');

app.timer('timerTrigger2', {
    schedule: '0 0 * * * *',
    handler: (myTimer, context) => {
        context.log('Timer function processed request.');
    }
});
