import { BlockEnum } from '@/app/components/workflow/types'
import { hasRetryNode } from './node'

describe('hasRetryNode', () => {
  it('supports question classifier nodes', () => {
    expect(hasRetryNode(BlockEnum.QuestionClassifier)).toBe(true)
  })

  it('does not support unrelated nodes', () => {
    expect(hasRetryNode(BlockEnum.Start)).toBe(false)
  })
})
