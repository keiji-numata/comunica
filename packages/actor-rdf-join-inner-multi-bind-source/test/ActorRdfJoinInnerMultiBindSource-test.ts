import { Bus } from '@comunica/core';
import { ActorRdfJoinMultiBindSource } from '../lib/ActorRdfJoinMultiBindSource';

describe('ActorRdfJoinInnerMultiBindSource', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfJoinMultiBindSource instance', () => {
    let actor: ActorRdfJoinMultiBindSource;

    beforeEach(() => {
      actor = new ActorRdfJoinMultiBindSource({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
