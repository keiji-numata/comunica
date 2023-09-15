import { Bus } from '@comunica/core';
import { ActorRdfJoinMultiSmallestFilterBindings } from '../lib/ActorRdfJoinMultiSmallestFilterBindings';

describe('ActorRdfJoinInnerMultiSmallestFilterBindings', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfJoinMultiSmallestFilterBindings instance', () => {
    let actor: ActorRdfJoinMultiSmallestFilterBindings;

    beforeEach(() => {
      actor = new ActorRdfJoinMultiSmallestFilterBindings({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
