import { Bus } from '@comunica/core';
import { ActorQuerySourceIdentifyHypermediaNone } from '../lib/ActorQuerySourceIdentifyHypermediaNone';

describe('ActorQuerySourceIdentifyHypermediaNone', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorQuerySourceIdentifyHypermediaNone instance', () => {
    let actor: ActorQuerySourceIdentifyHypermediaNone;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifyHypermediaNone({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
