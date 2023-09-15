import { Bus } from '@comunica/core';
import { ActorQuerySourceIdentifySerialized } from '../lib/ActorQuerySourceIdentifySerialized';

describe('ActorQuerySourceIdentifySerialized', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorQuerySourceIdentifySerialized instance', () => {
    let actor: ActorQuerySourceIdentifySerialized;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifySerialized({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
