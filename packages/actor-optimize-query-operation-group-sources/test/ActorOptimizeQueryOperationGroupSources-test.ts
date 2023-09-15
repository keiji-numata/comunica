import { Bus } from '@comunica/core';
import { ActorOptimizeQueryOperationGroupSources } from '../lib/ActorOptimizeQueryOperationGroupSources';

describe('ActorOptimizeQueryOperationGroupSources', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorOptimizeQueryOperationGroupSources instance', () => {
    let actor: ActorOptimizeQueryOperationGroupSources;

    beforeEach(() => {
      actor = new ActorOptimizeQueryOperationGroupSources({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
