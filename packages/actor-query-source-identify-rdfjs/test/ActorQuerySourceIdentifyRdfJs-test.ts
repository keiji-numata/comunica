import { Bus } from '@comunica/core';
import { ActorQuerySourceIdentifyRdfJs } from '../lib/ActorQuerySourceIdentifyRdfJs';

describe('ActorQuerySourceIdentifyRdfJs', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorQuerySourceIdentifyRdfJs instance', () => {
    let actor: ActorQuerySourceIdentifyRdfJs;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifyRdfJs({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
