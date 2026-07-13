import type { ReactNode } from 'react'
import type { NodeTracing } from '@/types/workflow'
import { fireEvent, render, screen } from '@testing-library/react'
import useTheme from '@/hooks/use-theme'
import { Theme } from '@/types/app'
import { BlockEnum, NodeRunningStatus } from '../../types'
import NodePanel from '../node'

vi.mock('@/hooks/use-theme', () => ({
  default: vi.fn(),
}))

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}))

vi.mock('@/context/i18n', () => ({
  useDocLink: () => (path: string) => path,
}))

vi.mock('@/app/components/workflow/nodes/_base/components/editor/code-editor', () => ({
  __esModule: true,
  default: ({ title, value, tip }: { title: ReactNode, value: unknown, tip?: ReactNode }) => (
    <section data-testid="code-editor">
      <div>{title}</div>
      <pre>{JSON.stringify(value)}</pre>
      {tip}
    </section>
  ),
}))

const mockUseTheme = vi.mocked(useTheme)

const createNodeInfo = (overrides: Partial<NodeTracing> = {}): NodeTracing => ({
  id: 'trace-node-1',
  index: 1,
  predecessor_node_id: '',
  node_id: 'node-1',
  node_type: BlockEnum.HttpRequest,
  title: 'HTTP Request',
  inputs: {},
  inputs_truncated: false,
  process_data: {},
  process_data_truncated: false,
  outputs_truncated: false,
  status: NodeRunningStatus.Succeeded,
  elapsed_time: 1,
  metadata: {
    iterator_length: 0,
    iterator_index: 0,
    loop_length: 0,
    loop_index: 0,
  },
  created_at: 0,
  created_by: {
    id: 'user-1',
    name: 'User',
    email: 'user@example.com',
  },
  finished_at: 1,
  ...overrides,
})

describe('NodePanel retry succeeded state', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockUseTheme.mockReturnValue({ theme: Theme.light } as ReturnType<typeof useTheme>)
  })

  it('should show retry succeeded warning and process data banner when a succeeded node has retry errors', () => {
    render(
      <NodePanel
        nodeInfo={createNodeInfo({
          process_data: {
            request: 'GET /test HTTP/1.1',
            retry_errors: [
              {
                retry_index: 1,
                error: 'temporary http error',
              },
            ],
          },
        })}
      />,
    )

    fireEvent.click(screen.getByText('HTTP Request'))

    expect(screen.getByText('nodes.common.retry.retrySuccessful')).toBeInTheDocument()
    expect(screen.getByText('nodes.common.retry.retryDetectedInProcessData')).toBeInTheDocument()
    expect(screen.getAllByTestId('code-editor').some(editor => editor.textContent?.includes('retry_errors'))).toBe(true)
  })
})
