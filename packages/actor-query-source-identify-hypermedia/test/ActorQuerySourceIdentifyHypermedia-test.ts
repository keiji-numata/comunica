import { Bus } from '@comunica/core';
import { ActorQuerySourceIdentifyHypermedia } from '../lib/ActorQuerySourceIdentifyHypermedia';

describe('ActorQuerySourceIdentifyHypermedia', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorQuerySourceIdentifyHypermedia instance', () => {
    let actor: ActorQuerySourceIdentifyHypermedia;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifyHypermedia({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
