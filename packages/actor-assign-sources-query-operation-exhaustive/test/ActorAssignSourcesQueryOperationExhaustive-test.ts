import { Bus } from '@comunica/core';
import { ActorAssignSourcesQueryOperationExhaustive } from '../lib/ActorAssignSourcesQueryOperationExhaustive';

describe('ActorAssignSourcesQueryOperationExhaustive', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorAssignSourcesQueryOperationExhaustive instance', () => {
    let actor: ActorAssignSourcesQueryOperationExhaustive;

    beforeEach(() => {
      actor = new ActorAssignSourcesQueryOperationExhaustive({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
