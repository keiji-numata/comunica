import { Bus } from '@comunica/core';
import { ActorContextPreprocessQuerySourceIdentify } from '../lib/ActorContextPreprocessQuerySourceIdentify';

describe('ActorContextPreprocessQuerySourceIdentify', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorContextPreprocessQuerySourceIdentify instance', () => {
    let actor: ActorContextPreprocessQuerySourceIdentify;

    beforeEach(() => {
      actor = new ActorContextPreprocessQuerySourceIdentify({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
